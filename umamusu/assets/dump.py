from collections import defaultdict
from io import BytesIO
from pathlib import Path

import UnityPy
from UnityPy.classes import Sprite, Texture2D
from UnityPy.files import ObjectReader
from tqdm import tqdm
from ..shared import meta_cursor, state
from . import logger
import struct
import sys

ABKey = bytes(
    [
        0x53, 0x2B, 0x46, 0x31, 0xE4, 0xA7, 0xB9, 0x47, 0x3E, 0x7C, 0xFB
    ])

def generate_keys(base_keys: bytes, key: int) -> bytes:
    base_len = len(base_keys)
    keys = bytearray(base_len * 8)

    # Convert int key to bytes (little-endian)
    key_bytes = struct.pack('<Q', key & 0xFFFFFFFFFFFFFFFF)
    # If system is big-endian, reverse to match C# BitConverter behavior
    if sys.byteorder != 'little':
        key_bytes = key_bytes[::-1]

    for i in range(base_len):
        b = base_keys[i]
        base_offset = i * 8
        for j in range(8):
            keys[base_offset + j] = b ^ key_bytes[j]

    return bytes(keys)

def list_valid_kinds():
    if not state.appdata_path.exists():
        logger.error(f"AppData folder does not exist: {state.storage_path}")
        return

    query = "SELECT DISTINCT m FROM a ORDER BY m"

    with meta_cursor() as cursor:
        cursor.execute(query)
        kinds = [row[0] for row in cursor.fetchall()]

    logger.info(f"Valid values of m: {kinds}")
    return kinds

def decrypt_uma_assetbundle(input_path: Path, keys: bytes, base_keys_len: int):
    HEADER_SIZE = 256
    with open(input_path, "rb") as f_in:
        data = f_in.read()
        decrypted = bytearray(data)
        total_len = len(decrypted)
        for i in range(HEADER_SIZE, total_len):
            decrypted[i] ^= keys[i % (base_keys_len * 8)]
        return decrypted

def assets_dump(args):
    if not state.appdata_path.exists():
        logger.error(f"AppData folder does not exist: {state.storage_path}")
        return

    where_query = ""
    if args.kind:
        # to filter out, add  AND n LIKE '%chara_stand______1_____' for standing portraits only
        where_query = "WHERE m IN ({})".format(",".join([f"'{k}'" for k in args.kind]))

    offset_query = ""
    if args.skip_i:
        offset_query = "OFFSET {}".format(args.skip_i)

    query = "SELECT n, h, m, e FROM a {} ORDER BY i {}".format(where_query, offset_query)

    with meta_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    dat_path = state.appdata_path / "dat"
    assets_path = state.storage_path / "assets"

    skipped = 0
    for i, (row_path, row_hash, row_kind, key) in enumerate(
        tqdm(rows, desc="Processing DB rows", unit="row", total=len(rows))
    ):
        fKey = generate_keys(ABKey, key)
        
        if row_path.startswith("/"):
            continue

        path = Path(row_path)
        dump_path = assets_path / path
        if args.skip_existing and dump_path.exists():
            skipped += 1
            continue

        appdata_file = dat_path / row_hash[:2] / row_hash
        if not appdata_file.exists():
            skipped += 1
            continue

        dump_path.mkdir(parents=True, exist_ok=True)

        try:
            decrypted_data = decrypt_uma_assetbundle(appdata_file, fKey, len(ABKey))
        except Exception as e:
            logger.error(f"Failed to decrypt {row_hash}: {e}")
            skipped += 1
            continue
    
        try:
            pack = UnityPy.load(BytesIO(decrypted_data))
            class_objects = defaultdict(list)
            for obj in pack.objects:
                class_objects[obj.get_class()].append(obj)

            if Texture2D in class_objects:
                texture_dump(class_objects, dump_path, row_kind)
            
        except Exception as e:
            logger.error(f"Failed to load: {row_hash} ({e})")
            skipped += 1
            continue

    logger.debug(f"finished processing {i} DB rows (skipped {skipped})")

def texture_dump(class_objects: dict[type, list[ObjectReader]], path: Path, kind: str):
    texture_images = []
    for i, texture_obj in enumerate(class_objects[Texture2D]):
        try:
            image = texture_obj.read().image
        except Exception:
            logger.error("failed to parse texture image: %s", path)
            continue

        image_name = Path(path.name)
        if texture_obj.container:
            image_name = Path(Path(texture_obj.container).name)

        texture_images.append((image, image_name))

    images = []
    if sprite_objs := class_objects.get(Sprite):
        # This is most likely an atlas
        if len(texture_images) != 1:
            logger.debug("found asset with multiple textures: %s", path)

        image, image_name = texture_images[0]
        images.append((image, image_name))
        for i, sprite_obj in enumerate(sprite_objs):
            sprite = sprite_obj.parse_as_dict()
            if rect := sprite.get("m_Rect"):
                x, y, h, w = rect["x"], rect["y"], rect["height"], rect["width"]
                # PIL and Unity treat height as starting from opposite sides
                sprite_img = image.crop(
                    (x, image.height - y - h, x + w, image.height - y)
                )
                sprite_name = sprite.get("m_Name")
                if not sprite_name:
                    sprite_name = f"{i}_{image_name}"

                images.append((sprite_img, sprite_name))
    else:
        for image, image_name in texture_images:
            if resize := image_resize(image_name.name, kind):
                image = image.resize(resize)

            images.append((image, image_name))

    names = set()
    for i, (image, image_name) in enumerate(images):
        # PIL fails to save if the file has no extension
        image_name = Path(image_name)
        if not image_name.suffix:
            image_name = image_name.with_suffix(".png")

        if image_name in names:
            logger.debug("found duplicate image name: %s %s", path, image_name)
            image_name = image_name.with_stem(f"{image_name.stem}_{i}")

        image.save(path / image_name)
        names.add(image_name)

def image_resize(name: str, kind: str):
    resize = None
    if kind == "supportcard":
        if name.startswith("support_card_s"):
            resize = (200, 200)
        elif name.startswith("support_thumb"):
            resize = (450, 600)
        elif name.startswith("tex_support_card"):
            resize = (450, 600)
    if kind == "gachaselect":
        if "cursor" in name:
            pass
        elif name.startswith("img_bnr_gacha"):
            resize = (512, 182)

    return resize

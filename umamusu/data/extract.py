import json

from ..shared import Status, master_cursor, state
from . import logger

def characard_extract(args):
    with master_cursor() as cursor:
        cursor.execute("""
            SELECT n."index", n.text
            FROM text_data n
            WHERE n.category = 4
              AND CAST(n."index" AS TEXT) LIKE '1%'
            ORDER BY n."index"
        """)
        text_rows = cursor.fetchall()

    chara_cards = []
    for index, text in text_rows:
        # derive chara_id from the first 4 digits of index
        chara_id = int(str(index)[:4])
        chara_cards.append(
            {
                "id": index,
                "chara_id": chara_id,
                "name": text,
            }
        )

    return [("characard.json", chara_cards)]


def supportcard_extract(args):
    with master_cursor() as cursor:
        # Step 1: Basic support card info
        cursor.execute("""
            SELECT c.id, c.rarity, c.command_id, c.start_date, n.text
            FROM support_card_data c
            JOIN text_data n ON n."index" = c.id AND n.category = 75
            ORDER BY c.rarity DESC, c.start_date, c.id
        """)
        support_card_rows = cursor.fetchall()

        # Step 2: Load stat type names (category 151)
        cursor.execute("""
            SELECT "index", text
            FROM text_data
            WHERE category = 151
        """)
        stat_name_map = {row[0]: row[1].lower().replace(' ', '_') for row in cursor.fetchall()}

        # Step 3: Load support_card_effect data
        # We find the max value across all limit_lv columns, ignoring -1
        cursor.execute("""
            SELECT id, type,
                    MAX(
                        limit_lv5, limit_lv10, limit_lv15, limit_lv20, limit_lv25,
                        limit_lv30, limit_lv35, limit_lv40, limit_lv45, limit_lv50
                    ) AS max_value
            FROM support_card_effect_table
        """)
        effect_rows = cursor.fetchall()

        # Aggregate main effect stats
        effect_stats = {}
        for card_id, stat_type, max_value in effect_rows:
            if card_id not in effect_stats:
                effect_stats[card_id] = {}
            stat_name = stat_name_map.get(stat_type, f"type_{stat_type}")
            effect_stats[card_id][stat_name] = effect_stats[card_id].get(stat_name, 0) + max_value

        # Step 4: Load unique effects (additive)
        cursor.execute("""
            SELECT id, type_0, value_0, type_1, value_1
            FROM support_card_unique_effect
        """)
        unique_rows = cursor.fetchall()

        for card_id, t0, v0, t1, v1 in unique_rows:
            if card_id not in effect_stats:
                effect_stats[card_id] = {}
            for t, v in ((t0, v0), (t1, v1)):
                if t is None or v is None or t == -1:
                    continue
                stat_name = stat_name_map.get(t, f"type_{t}")
                effect_stats[card_id][stat_name] = effect_stats[card_id].get(stat_name, 0) + v

    # Step 5: Combine results
    support_cards = []
    for id, rarity, type, start_date, name in support_card_rows:
        support_cards.append({
            "id": id,
            "name": name,
            "rarity": rarity,
            "type": type,
            "ts": start_date,
            "stats": effect_stats.get(id, {})
        })

    return [("supportcard.json", support_cards)]


def supportcard_extract_id_only(args):
    with master_cursor() as cursor:
        cursor.execute("""
            SELECT n."index", n.text
            FROM text_data n
            WHERE n.category = 75
            ORDER BY n."index"
        """)
        support_card_rows = cursor.fetchall()
    support_cards = []
    for index, text in support_card_rows:
        support_cards.append(
            {
                "id": index,
                "name": text,
            }
        )
    return [("supportcardidonly.json", support_cards)]


def factor_extract(args):
    with master_cursor() as cursor:
        cursor.execute("""
            SELECT f.factor_id, f.rarity, f.grade, f.factor_type, n.text, d.text
            FROM succession_factor f
            JOIN text_data n ON n."index" = f.factor_id AND n.category = 147
            JOIN text_data d ON d."index" = f.factor_id AND d.category = 172
        """)
        factor_rows = cursor.fetchall()

    factors = []
    for factor_id, rarity, grade, type, name, desc in factor_rows:
        factors.append(
            {
                "id": factor_id,
                "name": name,
                "description": desc,
                "rarity": rarity,
                "grade": grade,
                "type": type,
            }
        )

    return [("factor.json", factors)]


def skill_extract(args):
    with master_cursor() as cursor:
        cursor.execute("""
            SELECT s.id, s.rarity, s.skill_category, s.condition_1, s.condition_2, s.icon_id, n.text, d.text
            FROM skill_data s
            JOIN text_data n ON n."index" = s.id AND n.category = 47
            JOIN text_data d ON d."index" = s.id AND d.category = 48
        """)
        factor_rows = cursor.fetchall()

    skills = []
    for (
        skill_id,
        rarity,
        category,
        condition_1,
        condition_2,
        icon_id,
        name,
        desc,
    ) in factor_rows:
        skills.append(
            {
                "id": skill_id,
                "name": name,
                "description": desc,
                "rarity": rarity,
                "category": category,
                "condition_1": condition_1,
                "condition_2": condition_2,
                "icon_id": icon_id,
            }
        )

    return [("skill.json", skills)]


EXTRACTORS = {
    "supportcardidonly" : supportcard_extract_id_only,
    "supportcard": supportcard_extract,
    "characard": characard_extract,
    "factor": factor_extract,
    "skill": skill_extract,
}


def data_extract(args):
    kinds = args.kind
    if not kinds:
        kinds = list(EXTRACTORS.keys())

    data_path = state.storage_path / "data"
    data_path.mkdir(exist_ok=True)
    for kind in kinds:
        extractor = EXTRACTORS.get(kind)
        if not extractor:
            logger.error(f"invalid kind: {kind}")
            continue

        for data_filename, data in extractor(args):
            data_file = data_path / data_filename
            with data_file.open("w+") as file:
                json.dump(data, file, indent=2)

            logger.info(f"Extracted '{kind}' data to '{data_file}'!", status=Status.OK)

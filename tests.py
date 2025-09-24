from pprint import pprint
from db import Database
from bson import ObjectId
from datetime import datetime


def clean_doc(doc):
    """Convertit ObjectId et datetime en string pour affichage lisible."""
    if isinstance(doc, list):
        return [clean_doc(d) for d in doc]
    if isinstance(doc, dict):
        return {
            k: (
                str(v) if isinstance(v, ObjectId)
                else v.isoformat() if isinstance(v, datetime)
                else clean_doc(v) if isinstance(v, (dict, list))
                else v
            )
            for k, v in doc.items()
        }
    return doc


def log(title, data=None):
    """Affiche un titre + pprint formaté et clean."""
    print(f"\n--- {title} ---")
    if data is not None:
        pprint(clean_doc(data))


def safe_update_by_pid(db, collection, pid, update_fields, updated_by="tester"):
    """Sécurité : update uniquement si pid existe."""
    item = db.get_item_by_pid(collection, pid, fields=["pid"])
    if not item:
        print(f"Aucun item trouvé avec pid={pid} dans {collection}")
        return None
    return db.update_item_by_pid(collection, pid, update_fields, updated_by=updated_by)


def main():
    db = Database()
    log("✅ Connexion Mongo OK")

    # --- CREATE
    eve = db.create_item("users", {"name": "Eve", "email": "eve@example.com", "role": "qa"}, created_by="tester")
    eve_pid = eve["pid"]
    log("User créé (Eve)", eve)

    many = db.create_items("users", [
        {"name": "Fred", "email": "fred@example.com", "role": "dev"},
        {"name": "Gina", "email": "gina@example.com", "role": "dev"},
    ], created_by="tester")
    log("Plusieurs users créés", many)

    # --- GET
    log("User via pid (pid uniquement)", db.get_item_by_pid("users", eve_pid, fields=None))
    log("User via email (tous champs)", db.get_item_by_attr("users", {"email": "eve@example.com"}, fields=[]))
    log("User via email (name uniquement)", db.get_item_by_attr("users", {"email": "eve@example.com"}, fields=["name"]))

    # --- LOOKUP
    team = db.get_item_by_attr("teams", {}, fields=[], pipeline=[{
        "$lookup": {
            "from": "users",
            "localField": "members",
            "foreignField": "pid",
            "as": "members_info"
        }
    }])
    log("Team avec infos des membres", team)

    # --- UPDATE
    safe_update_by_pid(db, "users", eve_pid, {"name": "Eve QA"})
    log("Eve après update", db.get_item_by_pid("users", eve_pid, fields=[]))

    mod_count = db.update_items_by_attr("users", {"role": "dev"}, {"role": "engineer"}, updated_by="tester")
    log("Users modifiés (dev→engineer)", mod_count)

    before = db.get_item_by_pid("users", eve_pid, fields=["updated_at"])
    safe_update_by_pid(db, "users", eve_pid, {"role": "qa"})
    after = db.get_item_by_pid("users", eve_pid, fields=["updated_at"])
    log("updated_at", {"avant": before["updated_at"], "après": after["updated_at"]})

    # --- DELETE
    db.delete_item_by_pid("users", eve_pid)
    del_count = db.delete_items_by_attr("users", {"role": "engineer"})
    log("Users supprimés (role=engineer)", del_count)

    # --- ARRAYS
    t = db.get_item_by_attr("teams", {}, fields=["pid"])
    u = db.get_item_by_attr("users", {"email": "bob@example.com"}, fields=["pid"])
    if t and u:
        db.array_push_item_by_pid("teams", t["pid"], "members", u["pid"], updated_by="tester")
        db.array_pull_item_by_pid("teams", t["pid"], "members", u["pid"], updated_by="tester")

    proj = db.get_item_by_attr("projects", {}, fields=["pid", "tags"])
    if proj:
        db.array_push_item_by_pid("projects", proj["pid"], "tags", "hotfix", updated_by="tester")
        db.array_pull_item_by_pid("projects", proj["pid"], "tags", "hotfix", updated_by="tester")
        db.array_pull_item_by_attr("projects", {}, "tags", "urgent", updated_by="tester")

    # --- GET avancés
    any_user = db.get_item_by_attr("users", {}, fields=["pid"])
    if any_user:
        user_pid = any_user["pid"]
        items, stats = db.get_items(
            "projects",
            {},  
            fields=["name", "deadline", "teams", "tags", "budget"],
            sort={"deadline": 1},
            limit=50,
            return_stats=True,
            pipeline=[
                {"$lookup": {"from": "teams", "localField": "teams", "foreignField": "pid", "as": "teams_info"}},
                {"$match": {"teams_info.members": {"$in": [user_pid]}}},
            ],
        )
        log("Projets d’un user triés par deadline", {"items": items, "stats": stats})

    # Pagination
    page1, stats1 = db.get_items("users", {}, fields=["name", "email"], sort={"name": 1}, skip=0, limit=2, return_stats=True)
    page2, stats2 = db.get_items("users", {}, fields=["name", "email"], sort={"name": 1}, skip=2, limit=2, return_stats=True)
    log("Pagination Users (page1)", {"data": page1, "stats": stats1})
    log("Pagination Users (page2)", {"data": page2, "stats": stats2})

    # Compter projets urgent
    _, urgent_stats = db.get_items("projects", {}, fields=["pid"], return_stats=True, pipeline=[{"$match": {"tags": "urgent"}}])
    log("Nombre de projets urgent", urgent_stats["itemsCount"])


if __name__ == "__main__":
    main()

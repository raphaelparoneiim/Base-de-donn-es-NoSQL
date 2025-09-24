import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database as _MongoDatabase
from dotenv import load_dotenv

# Charger le .env
load_dotenv()

# DEBUG : afficher les variables lues
print("DEBUG MONGODB_URI =", os.getenv("MONGODB_URI"))
print("DEBUG MONGODB_DB_NAME =", os.getenv("MONGODB_DB_NAME"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_fields(fields: Optional[List[str]]) -> Optional[Dict[str, int]]:
    """
    Règles:
      - fields is None -> ne retourner que pid
      - fields == []   -> tous les champs (=> projection None)
      - fields == ["xx","yy"] -> retourner pid, xx, yy
    """
    if fields is None:
        return {"pid": 1}
    if isinstance(fields, list) and len(fields) == 0:
        return None
    proj = {"pid": 1}
    for f in fields:
        proj[f] = 1
    return proj


class Database:
    """
    Classe centrale des opérations MongoDB (avec pymongo + agrégations).
    """

    def __init__(self, uri: Optional[str] = None, db_name: Optional[str] = None, server_selection_timeout_ms: int = 5000):
        self._uri = uri or os.getenv("MONGODB_URI")
        self._db_name = db_name or os.getenv("MONGODB_DB_NAME")

        if not self._uri or not self._db_name:
            raise ValueError("MONGODB_URI et/ou MONGODB_DB_NAME manquants (check .env).")

        self._client = MongoClient(self._uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
        # Test connexion
        self._client.server_info()

        self._db: _MongoDatabase = self._client.get_database(self._db_name)

    # -------- Helpers
    def col(self, table: str) -> Collection:
        return self._db[table]

    def _with_audit_on_create(self, doc: Dict[str, Any], created_by: Optional[str]) -> Dict[str, Any]:
        now = utcnow()
        return {
            **doc,
            "pid": uuid.uuid4().hex,
            "created_at": now,
            "updated_at": now,
            **({"created_by": created_by} if created_by else {}),
        }

    def _with_audit_on_update(self, updates: Dict[str, Any], updated_by: Optional[str]) -> Dict[str, Any]:
        set_part = {"updated_at": utcnow(), **updates}
        if updated_by is not None:
            set_part["updated_by"] = updated_by
        return {"$set": set_part}

    # ======================
    # Partie 2 - CREATE
    # ======================
    def create_item(self, table: str, item: Dict[str, Any], created_by: Optional[str] = None) -> Dict[str, Any]:
        doc = self._with_audit_on_create(item, created_by)
        self.col(table).insert_one(doc)
        return {"pid": doc["pid"]}

    def create_items(self, table: str, items: List[Dict[str, Any]], created_by: Optional[str] = None) -> List[Dict[str, Any]]:
        docs = [self._with_audit_on_create(it, created_by) for it in items]
        if docs:
            self.col(table).insert_many(docs)
        return [{"pid": d["pid"]} for d in docs]

    # ======================
    # Partie 4 - UPDATE
    # ======================
    def update_items_by_attr(self, table: str, attributes: Dict[str, Any], items_data: Dict[str, Any], updated_by: Optional[str] = None) -> int:
        res = self.col(table).update_many(attributes, self._with_audit_on_update(items_data, updated_by))
        return res.modified_count

    def update_items_by_pids(self, table: str, pids: List[str], items_data: Dict[str, Any], updated_by: Optional[str] = None) -> int:
        res = self.col(table).update_many({"pid": {"$in": pids}}, self._with_audit_on_update(items_data, updated_by))
        return res.modified_count

    def update_item_by_attr(self, table: str, attributes: Dict[str, Any], item_data: Dict[str, Any], updated_by: Optional[str] = None) -> bool:
        res = self.col(table).update_one(attributes, self._with_audit_on_update(item_data, updated_by))
        return res.modified_count > 0

    def update_item_by_pid(self, table: str, pid: str, item_data: Dict[str, Any], updated_by: Optional[str] = None) -> bool:
        res = self.col(table).update_one({"pid": pid}, self._with_audit_on_update(item_data, updated_by))
        return res.modified_count > 0

    # ======================
    # Partie 5 - GET simples
    # ======================
    def get_item_by_attr(
        self,
        table: str,
        attributes: Dict[str, Any],
        fields: Optional[List[str]] = None,
        pipeline: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        proj = _normalize_fields(fields)
        stages: List[Dict[str, Any]] = [{"$match": attributes}]
        if proj is not None:
            stages.append({"$project": proj})
        if pipeline:
            stages.extend(pipeline)
        stages.append({"$limit": 1})
        cursor = self.col(table).aggregate(stages)
        return next(cursor, None)

    def get_item_by_pid(
        self,
        table: str,
        pid: str,
        fields: Optional[List[str]] = None,
        pipeline: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_item_by_attr(table, {"pid": pid}, fields=fields, pipeline=pipeline)

    # ======================
    # Partie 6 - DELETE
    # ======================
    def delete_items_by_attr(self, table: str, attributes: Dict[str, Any]) -> int:
        res = self.col(table).delete_many(attributes)
        return res.deleted_count

    def delete_items_by_pids(self, table: str, pids: List[str]) -> int:
        res = self.col(table).delete_many({"pid": {"$in": pids}})
        return res.deleted_count

    def delete_item_by_attr(self, table: str, attributes: Dict[str, Any]) -> bool:
        res = self.col(table).delete_one(attributes)
        return res.deleted_count > 0

    def delete_item_by_pid(self, table: str, pid: str) -> bool:
        res = self.col(table).delete_one({"pid": pid})
        return res.deleted_count > 0

    # ======================
    # Partie 7 - ARRAYS
    # ======================
    def array_push_item_by_attr(self, table: str, attributes: Dict[str, Any], array: str, new_item: Any, updated_by: Optional[str] = None) -> int:
        res = self.col(table).update_many(
            attributes,
            {"$addToSet": {array: new_item}, **self._with_audit_on_update({}, updated_by)}
        )
        return res.modified_count

    def array_push_item_by_pid(self, table: str, pid: str, array: str, new_item: Any, updated_by: Optional[str] = None) -> bool:
        res = self.col(table).update_one(
            {"pid": pid},
            {"$addToSet": {array: new_item}, **self._with_audit_on_update({}, updated_by)}
        )
        return res.modified_count > 0

    def array_pull_item_by_attr(self, table: str, attributes: Dict[str, Any], array: str, item_attr: Union[Any, Dict[str, Any]], updated_by: Optional[str] = None) -> int:
        res = self.col(table).update_many(
            attributes,
            {"$pull": {array: item_attr}, **self._with_audit_on_update({}, updated_by)}
        )
        return res.modified_count

    def array_pull_item_by_pid(self, table: str, pid: str, array: str, item_attr: Union[Any, Dict[str, Any]], updated_by: Optional[str] = None) -> bool:
        res = self.col(table).update_one(
            {"pid": pid},
            {"$pull": {array: item_attr}, **self._with_audit_on_update({}, updated_by)}
        )
        return res.modified_count > 0

    # ======================
    # Partie 8 - GET avancée
    # ======================
    def get_items(
        self,
        table: str,
        attributes: Dict[str, Any],
        fields: Optional[List[str]] = None,
        sort: Optional[Dict[str, int]] = None,
        skip: int = 0,
        limit: Optional[int] = None,
        return_stats: bool = False,
        pipeline: Optional[List[Dict[str, Any]]] = None,
    ) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        proj = _normalize_fields(fields)
        stages: List[Dict[str, Any]] = [{"$match": attributes}]
        if proj is not None:
            stages.append({"$project": proj})
        if pipeline:
            stages.extend(pipeline)
        if sort:
            stages.append({"$sort": sort})
        if skip:
            stages.append({"$skip": int(skip)})
        if limit is not None:
            stages.append({"$limit": int(limit)})

        items = list(self.col(table).aggregate(stages))

        if not return_stats:
            return items

        count_stages = [{"$match": attributes}]
        if proj is not None:
            count_stages.append({"$project": proj})
        if pipeline:
            count_stages.extend(pipeline)
        count_stages.append({"$count": "count"})

        total_cursor = self.col(table).aggregate(count_stages)
        total_doc = next(total_cursor, {"count": 0})
        total = int(total_doc.get("count", 0))

        stats = {
            "itemsCount": total,
            "pageSize": limit if limit is not None else total,
            "firstIndexReturned": skip if total > 0 else None,
            "pagesCount": ((total + limit - 1) // limit) if limit else 1,
        }
        return items, stats

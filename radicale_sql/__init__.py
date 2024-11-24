#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import binascii
import datetime
import zoneinfo
import uuid
import re
import string
import itertools
import json
from hashlib import sha256
from typing import Optional, Union, Tuple, Iterable, Iterator, Mapping, Set
import radicale.types
from radicale.auth import BaseAuth
from radicale.rights import BaseRights
from radicale.storage import BaseStorage, BaseCollection
from radicale.log import logger
from radicale import item as radicale_item
import requests
import sqlalchemy as sa
import xml.etree.ElementTree as ET
from functools import lru_cache
import time

from . import db

PLUGIN_CONFIG_SCHEMA = {
    "storage": {
        "db_url": {
            "value": "",
            "type": str,
        },
    },
}


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


@lru_cache(maxsize=50)
def my_expensive_function(email, password, ttl_hash=None):
    del ttl_hash  # to emphasize we don't use it and to shut pylint up

    # check email format
    if "@" not in email:
        return ""

    # post request to url
    response = requests.post(
        "https://api.jaewon.co.kr/_internal/check/credential",
        json={
            "email": email,
            "password": password,
        },
    )
    # 204 is ok, otherwise not
    if response.status_code >= 200 and response.status_code < 300:
        return email
    else:
        return ""


def get_ttl_hash(seconds=60):
    """Return the same value withing `seconds` time period"""
    return round(time.time() / seconds)


class Rights(BaseRights):
    def authorization(self, user: str, path: str) -> str:
        if user == "":
            return ""

        if path == "/":
            return "R"
        elif path == "/domain/":
            return "R"
        elif path == ("/" + user + "/"):
            return "RW"
        elif path.startswith("/" + user + "/"):
            return "rw"
        elif bool(re.match(r"/[^/]+/[^/]+", path)):
            return "r"

        return ""


class Auth(BaseAuth):
    def login(self, login, password):
        return my_expensive_function(login, password, ttl_hash=get_ttl_hash())


class Item(radicale_item.Item):

    def __init__(
        self,
        *args,
        last_modified: Optional[Union[str, datetime.datetime]] = None,
        **kwargs,
    ):
        if last_modified is not None and isinstance(last_modified, datetime.datetime):
            last_modified = last_modified.astimezone(
                tz=zoneinfo.ZoneInfo("GMT")
            ).strftime("%a, %d %b %Y %H:%M:%S GMT")
        super().__init__(*args, last_modified=last_modified, **kwargs)


class Collection(BaseCollection):
    def __init__(self, storage: "Storage", id: uuid.UUID, path: str):
        self._storage = storage
        self._id = id
        self._path = path
        self._meta = None
        self._updated_at = None

    def __repr__(self) -> str:
        return f"Collection(id={self._id}, path={self._path})"

    @property
    def path(self) -> str:
        return self._path

    def _row_to_item(self, row) -> "radicale_item.Item":
        return Item(
            collection=self,
            href=row.name,
            last_modified=datetime.datetime.fromtimestamp(
                row.modified_at / 1000.0, datetime.UTC
            ),
            text=row.data.decode(),
        )

    def _get_multi(
        self, hrefs: Iterable[str], *, connection
    ) -> Iterable[Tuple[str, Optional["radicale_item.Item"]]]:
        item_table = self._storage._meta.tables["cas.item"]
        hrefs_ = list(hrefs)
        # hrefs_ = [(x,) for x in hrefs]
        if not hrefs_:
            return []
        select_stmt = (
            sa.select(
                item_table.c,
            )
            .select_from(
                item_table,
            )
            .where(
                sa.and_(
                    item_table.c.collection_id == self._id,
                    item_table.c.name.in_(hrefs_),
                ),
            )
        )
        l = []
        for row in connection.execute(select_stmt):
            l += [(row.name, self._row_to_item(row))]
        hrefs_set = set(hrefs_)
        hrefs_set_have = set([x[0] for x in l])
        l += [(x, None) for x in (hrefs_set - hrefs_set_have)]
        return l

    def get_multi(
        self, hrefs: Iterable[str]
    ) -> Iterable[Tuple[str, Optional["radicale_item.Item"]]]:
        with self._storage._engine.begin() as c:
            return self._get_multi(hrefs=hrefs, connection=c)

    def _get_all(self, *, connection) -> Iterator["radicale_item.Item"]:
        item_table = self._storage._meta.tables["cas.item"]
        select_stmt = (
            sa.select(
                item_table.c,
            )
            .select_from(
                item_table,
            )
            .where(
                item_table.c.collection_id == self._id,
            )
        )
        for row in connection.execute(select_stmt):
            yield self._row_to_item(row)

    def _get_contains(self, text) -> Iterator["radicale_item.Item"]:
        with self._storage._engine.begin() as c:
            item_table = self._storage._meta.tables["cas.item"]
            select_stmt = (
                sa.select(
                    item_table.c,
                )
                .select_from(
                    item_table,
                )
                .where(
                    item_table.c.collection_id == self._id,
                )
                .where(
                    item_table.c.data.contains(text.encode("utf-8")),
                )
            )
            for row in c.execute(select_stmt):
                yield self._row_to_item(row)

    def get_all(self) -> Iterator["radicale_item.Item"]:
        with self._storage._engine.begin() as c:
            for i in self._get_all(connection=c):
                yield i

    def _upload(
        self, href: str, item: "radicale_item.Item", *, connection
    ) -> "radicale_item.Item":

        if href.endswith(".vcf"):
            item_id = href.split(".vcf")[0]
            if is_valid_uuid(item_id):
                item_id = uuid.UUID(item_id)
            else:
                item_id = uuid.uuid4()
                href = str(item_id) + ".vcf"
            item.uid = str(item_id)
        elif href.endswith(".ics"):
            item_id = href.split(".ics")[0]
            if is_valid_uuid(item_id):
                item_id = uuid.UUID(item_id)
            else:
                item_id = uuid.uuid4()
                href = str(item_id) + ".ics"
        else:
            raise ValueError("Invalid file extension")

        item_table = self._storage._meta.tables["cas.item"]

        parsed_data = {
            "full_name": None,
            "prefix": None,
            "suffix": None,
            "first_name": None,
            "last_name": None,
            "middle_name": None,
            "phone_number": None,
            "ext_number": None,
            "company": None,
            "title": None,
            "department": None,
            "categories": [],
        }
        data = item.serialize()
        if data.startswith("BEGIN:VCARD"):
            vcard = item.vobject_item
            # Full Name
            if hasattr(vcard, "fn"):
                parsed_data["full_name"] = vcard.fn.value

            # Name components
            if hasattr(vcard, "n"):
                name = vcard.n.value
                parsed_data["prefix"] = name.prefix
                parsed_data["first_name"] = name.given
                parsed_data["middle_name"] = name.additional
                parsed_data["last_name"] = name.family
                parsed_data["suffix"] = name.suffix

            # Phone number
            if hasattr(vcard, "tel"):
                tel = vcard.tel.value
                parsed_data["phone_number"] = tel.split(";")[0]  # 전화번호
                if "ext=" in tel:
                    parsed_data["ext_number"] = tel.split("ext=")[-1]  # 내선번호

            # Organization (Company and Department)
            if hasattr(vcard, "org"):
                org = vcard.org.value
                if len(org) > 0:
                    parsed_data["company"] = org[0]  # 회사
                if len(org) > 1:
                    parsed_data["department"] = org[1]  # 부서

            # Title
            if hasattr(vcard, "title"):
                parsed_data["title"] = vcard.title.value

            # Categories
            if hasattr(vcard, "categories"):
                parsed_data["categories"] = vcard.categories.value

            # iterate over parsed_data, replace blank or space to None
            for key, value in parsed_data.items():
                if key == "categories":
                    continue
                if value is not None:
                    parsed_data[key] = value.strip()
                    if parsed_data[key] == "":
                        parsed_data[key] = None

        item_serialized = data.encode()
        select_stmt = (
            sa.select(
                item_table.c,
            )
            .select_from(
                item_table,
            )
            .where(
                sa.and_(
                    item_table.c.collection_id == self._id,
                    item_table.c.name == href,
                ),
            )
        )
        insert_stmt = sa.insert(
            item_table,
        ).values(
            id=item_id,
            collection_id=self._id,
            name=href,
            data=item_serialized,
            full_name=parsed_data["full_name"],
            prefix=parsed_data["prefix"],
            suffix=parsed_data["suffix"],
            first_name=parsed_data["first_name"],
            last_name=parsed_data["last_name"],
            middle_name=parsed_data["middle_name"],
            phone_number=parsed_data["phone_number"],
            ext_number=parsed_data["ext_number"],
            company=parsed_data["company"],
            title=parsed_data["title"],
            department=parsed_data["department"],
            categories=parsed_data["categories"],
        )
        update_stmt = (
            sa.update(
                item_table,
            )
            .values(
                data=item_serialized,
                full_name=parsed_data["full_name"],
                prefix=parsed_data["prefix"],
                suffix=parsed_data["suffix"],
                first_name=parsed_data["first_name"],
                last_name=parsed_data["last_name"],
                middle_name=parsed_data["middle_name"],
                phone_number=parsed_data["phone_number"],
                ext_number=parsed_data["ext_number"],
                company=parsed_data["company"],
                title=parsed_data["title"],
                department=parsed_data["department"],
                categories=parsed_data["categories"],
            )
            .where(
                sa.and_(
                    item_table.c.collection_id == self._id,
                    item_table.c.name == href,
                    item_table.c.id == item_id,
                ),
            )
        )
        if connection.execute(select_stmt).one_or_none() is None:
            connection.execute(insert_stmt)
            self._storage._collection_updated(self._id, connection=connection)
        else:
            connection.execute(update_stmt)
            self._storage._item_updated(self._id, href, connection=connection)
        self._update_history_etag(href, item, connection=connection)
        res = list(self._get_multi([href], connection=connection))[0][1]
        assert res is not None
        return res

    def upload(self, href: str, item: "radicale_item.Item") -> "radicale_item.Item":
        with self._storage._engine.begin() as c:
            return self._upload(href, item, connection=c)

    def _delete(self, *, connection, href: Optional[str] = None) -> None:
        collection_table = self._storage._meta.tables["cas.collection"]
        item_table = self._storage._meta.tables["cas.item"]
        if href is None:
            delete_stmt = sa.delete(
                collection_table,
            ).where(
                collection_table.c.id == self._id,
            )
        else:
            delete_stmt = sa.delete(
                item_table,
            ).where(
                sa.and_(
                    item_table.c.collection_id == self._id,
                    item_table.c.name == href,
                ),
            )
            self._storage._item_updated(self._id, href, connection=connection)
        connection.execute(delete_stmt)

    def delete(self, href: Optional[str] = None) -> None:
        with self._storage._engine.begin() as c:
            return self._delete(connection=c, href=href)

    def _get_meta(
        self, *, connection, key: Optional[str] = None
    ) -> Union[Mapping[str, str], Optional[str]]:
        collection_metadata = self._storage._meta.tables["cas.collection_metadata"]
        select_meta = (
            sa.select(
                collection_metadata.c.key,
                collection_metadata.c.value,
            )
            .select_from(
                collection_metadata,
            )
            .where(
                collection_metadata.c.collection_id == self._id,
            )
        )
        if key is not None:
            select_meta = select_meta.where(
                collection_metadata.c.key == key,
            )
        metadata = {}
        for row in connection.execute(select_meta):
            metadata[row.key] = row.value
        if key is not None:
            return metadata.get(key)
        return metadata

    def get_meta(
        self, key: Optional[str] = None
    ) -> Union[Mapping[str, str], Optional[str]]:
        #  valid 5 mins
        if (
            self._meta is None
            or self._updated_at is None
            or self._updated_at
            < int(datetime.datetime.now(datetime.UTC).timestamp() * 1000.0)
            - (5 * 60 * 1000)
        ):
            with self._storage._engine.begin() as c:
                collection_metadata = self._storage._meta.tables[
                    "cas.collection_metadata"
                ]
                select_meta = (
                    sa.select(
                        collection_metadata.c.key,
                        collection_metadata.c.value,
                    )
                    .select_from(
                        collection_metadata,
                    )
                    .where(
                        collection_metadata.c.collection_id == self._id,
                    )
                )
                metadata = {}
                for row in c.execute(select_meta):
                    metadata[row.key] = row.value

                if metadata:
                    self._updated_at = int(
                        datetime.datetime.now(datetime.UTC).timestamp() * 1000.0
                    )

                self._meta = metadata

        if key is not None:
            return self._meta.get(key)
        return self._meta

    def _set_meta(self, props: Mapping[str, str], *, connection) -> None:
        collection_metadata = self._storage._meta.tables["cas.collection_metadata"]
        delete_stmt = sa.delete(
            collection_metadata,
        ).where(
            collection_metadata.c.collection_id == self._id,
        )
        insert_stmt = sa.insert(
            collection_metadata,
        ).values(
            [dict(collection_id=self._id, key=k, value=v) for k, v in props.items()]
        )
        connection.execute(delete_stmt)
        connection.execute(insert_stmt)
        self._storage._collection_updated(self._id, connection=connection)

    def set_meta(self, props: Mapping[str, str]) -> None:
        with self._storage._engine.begin() as c:
            return self._set_meta(props, connection=c)

    def _last_modified(self, *, connection) -> str:
        collection = self._storage._meta.tables["cas.collection"]
        select_stmt = (
            sa.select(
                collection.c.modified_at,
            )
            .select_from(
                collection,
            )
            .where(
                collection.c.id == self._id,
            )
        )
        c = connection.execute(select_stmt).one()
        return datetime.datetime.fromtimestamp(
            c.modified_at / 1000.0, datetime.UTC
        ).strftime("%a, %d %b %Y %H:%M:%S GMT")

    @property
    def last_modified(self):
        with self._storage._engine.begin() as c:
            return self._last_modified(connection=c)

    def _update_history_etag(
        self, href: str, item: Optional["radicale_item.Item"], *, connection
    ) -> str:
        item_history_table = self._storage._meta.tables["cas.item_history"]
        select_etag_stmt = (
            sa.select(
                item_history_table.c,
            )
            .select_from(
                item_history_table,
            )
            .where(
                sa.and_(
                    item_history_table.c.collection_id == self._id,
                    item_history_table.c.name == href,
                ),
            )
        )
        exists: bool
        item_history = connection.execute(select_etag_stmt).one_or_none()
        if item_history is not None:
            exists = True
            cache_etag = item_history.etag
            history_etag = item_history.history_etag
        else:
            exists = False
            cache_etag = ""
            history_etag = binascii.hexlify(os.urandom(16)).decode("ascii")
        etag = item.etag if item else ""
        if etag != cache_etag:
            history_etag = radicale_item.get_etag(history_etag + "/" + etag).strip('"')
            if exists:
                upsert = (
                    sa.update(
                        item_history_table,
                    )
                    .values(
                        etag=etag,
                        history_etag=history_etag,
                    )
                    .where(
                        sa.and_(
                            item_history_table.c.collection_id == self._id,
                            item_history_table.c.name == href,
                        ),
                    )
                )
            else:
                upsert = sa.insert(
                    item_history_table,
                ).values(
                    collection_id=self._id,
                    name=href,
                    etag=etag,
                    history_etag=history_etag,
                )
            connection.execute(upsert)
        return history_etag

    def _get_deleted_history_refs(self, *, connection):
        item_table = self._storage._meta.tables["cas.item"]
        item_history_table = self._storage._meta.tables["cas.item_history"]
        select_stmt = (
            sa.select(
                item_history_table.c.name,
            )
            .select_from(
                item_history_table.join(
                    item_table,
                    sa.and_(
                        item_history_table.c.collection_id
                        == item_table.c.collection_id,
                        item_history_table.c.name == item_table.c.name,
                    ),
                    isouter=True,
                ),
            )
            .where(
                sa.and_(
                    item_history_table.c.collection_id == self._id,
                    item_table.c.id == None,
                ),
            )
        )
        for row in connection.execute(select_stmt):
            yield row.name

    def _delete_history_refs(self, *, connection):
        item_history_table = self._storage._meta.tables["cas.item_history"]
        delete_stmt = sa.delete(
            item_history_table,
        ).where(
            sa.and_(
                item_history_table.c.href.in_(
                    list(self._get_deleted_history_refs(connection=connection))
                ),
                item_history_table.c.collection_id == self._id,
                item_history_table.c.modified_at
                < int(
                    (
                        datetime.datetime.now()
                        - datetime.timedelta(
                            seconds=self._storage.configuration.get(
                                "storage", "max_sync_token_age"
                            )
                        )
                    ).timestamp()
                    * 1000.0
                ),
            ),
        )
        connection.execute(delete_stmt)

    def _sync(self, *, connection, old_token: str = "") -> Tuple[str, Iterable[str]]:
        # Parts of this method have been taken from
        # https://github.com/Kozea/Radicale/blob/6a56a6026f6ec463d6eb77da29e03c48c0c736c6/radicale/storage/multifilesystem/sync.py
        _prefix = "http://radicale.org/ns/sync/"
        collection_state_table = self._storage._meta.tables["cas.collection_state"]

        def check_token_name(token_name: str) -> bool:
            if len(token_name) != 64:
                return False
            for c in token_name:
                if c not in string.hexdigits.lower():
                    return False
            return True

        old_token_name = ""
        if old_token:
            if not old_token.startswith(_prefix):
                raise ValueError(f"Malformed token: {old_token}")
            old_token_name = old_token[len(_prefix) :]
            if not check_token_name(old_token_name):
                raise ValueError(f"Malformed token: {old_token}")

        # compute new state
        state = {}
        token_name_hash = sha256()
        for href, item in itertools.chain(
            ((item.href, item) for item in self._get_all(connection=connection)),
            (
                (href, None)
                for href in self._get_deleted_history_refs(connection=connection)
            ),
        ):
            assert isinstance(href, str)
            if href in state:
                # we don't want to overwrite states
                # this could happen with another storage collection
                # which doesn't store the items itself, but
                # derives them from another one
                continue
            history_etag = self._update_history_etag(href, item, connection=connection)
            state[href] = history_etag
            token_name_hash.update((href + "/" + history_etag).encode())
        token_name = token_name_hash.hexdigest()
        token = _prefix + token_name

        # if new state hasn't changed: dont send any updates
        if token_name == old_token_name:
            return token, ()

        # load old state
        old_state = {}
        if old_token_name:
            select_stmt = (
                sa.select(
                    collection_state_table.c,
                )
                .select_from(
                    collection_state_table,
                )
                .where(
                    sa.and_(
                        collection_state_table.c.collection_id == self._id,
                        collection_state_table.c.name == old_token_name,
                    ),
                )
            )
            state_row = connection.execute(select_stmt).one_or_none()
            old_state = (
                json.loads(state_row.state.decode()) if state_row is not None else {}
            )

        # store new state
        select_new_state = (
            sa.select(
                collection_state_table.c,
            )
            .select_from(
                collection_state_table,
            )
            .where(
                collection_state_table.c.collection_id == self._id,
                collection_state_table.c.name == token_name,
            )
        )
        if connection.execute(select_new_state).one_or_none() is None:
            insert_stmt = sa.insert(
                collection_state_table,
            ).values(
                collection_id=self._id,
                name=token_name,
                state=json.dumps(state).encode(),
            )
            connection.execute(insert_stmt)

        changes = []
        for href, history_etag in state.items():
            if history_etag != old_state.get(href):
                changes += [href]
        for href, history_etag in old_state.items():
            if href not in state:
                changes += [href]

        return token, changes

    def sync(self, old_token: str = "") -> Tuple[str, Iterable[str]]:
        with self._storage._engine.begin() as c:
            return self._sync(connection=c, old_token=old_token)

    def get_filtered(
        self, filters: Iterable[ET.Element]
    ) -> Iterable[Tuple["radicale_item.Item", bool]]:
        if (
            len(filters) == 1
            and len(filters[0]) == 1
            and len(filters[0][0]) == 1
            and "text-match" in filters[0][0][0].tag
            and filters[0][0][0].get("match-type") in ["contains", "equals"]
        ):
            for item in self._get_contains(filters[0][0][0].text):
                yield item, False
        else:
            yield from super().get_filtered(filters)

    def has_uid(self, uid: str) -> bool:
        items = self._get_contains(uid)
        for item in items:
            if item.uid == uid:
                return True
        return False


def create_collection(*args, **kwargs) -> Collection:
    c = Collection
    return c(*args, **kwargs)


class Storage(BaseStorage):

    def __init__(self, configuration: "radicale.config.Configuration"):
        super().__init__(configuration)
        self._meta = db.create_meta()
        self._engine, self._root_collection = db.create(
            self.configuration.get("storage", "url"), self._meta
        )
        with self._engine.begin() as c:
            collection_table = self._meta.tables["cas.collection"]
            select_stmt = (
                sa.select(
                    collection_table.c,
                )
                .select_from(
                    collection_table,
                )
                .where(
                    sa.and_(
                        collection_table.c.parent_id == self._root_collection.id,
                        collection_table.c.name == "domain",
                    ),
                )
            )
            self._domain_collection = c.execute(select_stmt).one()

    def _split_path(self, path: str):
        path_parts = path.split("/")
        if path_parts[0] == "":
            path_parts = path_parts[1:]
        if path_parts[-1] == "":
            path_parts = path_parts[:-1]
        return path_parts

    def _get_collection(self, id, *, connection) -> "BaseCollection":
        collection_table = self._meta.tables["cas.collection"]
        select_stmt = sa.select(
            collection_table.c,
        ).where(
            collection_table.c.id == id,
        )
        row = connection.execute(select_stmt).one()
        # TODO: path
        return create_collection(self, id, "")

    def _collection_updated(self, collection_id, *, connection):
        collection_table = self._meta.tables["cas.collection"]
        connection.execute(
            sa.update(
                collection_table,
            )
            .values(
                modified_at=int(
                    datetime.datetime.now(datetime.UTC).timestamp() * 1000.0
                ),
            )
            .where(
                collection_table.c.id == collection_id,
            )
        )

    def _item_updated(self, collection_id: uuid.UUID, href: str, *, connection):
        item_table = self._meta.tables["cas.item"]
        item_row = connection.execute(
            sa.update(
                item_table,
            )
            .values(
                modified_at=int(
                    datetime.datetime.now(datetime.UTC).timestamp() * 1000.0
                ),
            )
            .where(
                sa.and_(
                    item_table.c.collection_id == collection_id,
                    item_table.c.name == href,
                ),
            )
            .returning(item_table.c)
        ).one()
        self._collection_updated(item_row.collection_id, connection=connection)

    def _discover(
        self, path: str, *, connection, depth: str = "0"
    ) -> Iterable["radicale.types.CollectionOrItem"]:
        if path == "/":
            return [create_collection(self, self._root_collection.id, "")]
        path_parts = self._split_path(path)

        collection_table = self._meta.tables["cas.collection"]
        item_table = self._meta.tables["cas.item"]

        select_collection_or_item = sa.select(
            collection_table.c.id,
            collection_table.c.parent_id.label("parent_id"),
            collection_table.c.modified_at,
            collection_table.c.name,
            sa.literal(None, sa.LargeBinary()).label("data"),
            sa.literal("collection", sa.String(16)).label("type_"),
        ).union_all(
            sa.select(
                item_table.c.id,
                item_table.c.collection_id.label("parent_id"),
                item_table.c.modified_at,
                item_table.c.name,
                item_table.c.data,
                sa.literal("item", sa.String(16)).label("type_"),
            ).select_from(item_table)
        )

        i = 0
        select_from = select_collection_or_item.alias("data")
        aliases = [select_from]
        for path in path_parts[::-1]:
            aliases += [select_collection_or_item.alias(f"t{i}")]
            i += 1
            select_from = select_from.join(
                aliases[-1],
                sa.and_(
                    aliases[-2].c.parent_id == aliases[-1].c.id,
                    aliases[-2].c.name == path,
                ),
            )
        select_stmt = (
            sa.select(
                aliases[0].c,
            )
            .select_from(select_from)
            .where(
                aliases[-1].c.parent_id == None,
            )
        )

        l = []
        self_collection = connection.execute(select_stmt).one_or_none()
        if self_collection is None:
            # None found
            return []
        if self_collection.type_ != "collection":
            # Item found
            return [
                Item(
                    collection=self._get_collection(
                        self_collection.parent_id, connection=connection
                    ),
                    href=self_collection.name,
                    last_modified=datetime.datetime.fromtimestamp(
                        self_collection.modified_at / 1000.0, datetime.UTC
                    ),
                    text=self_collection.data.decode(),
                )
            ]

        # collection found
        self_collection = create_collection(
            self, self_collection.id, "/".join(path_parts)
        )
        l += [self_collection]
        # collection should list contents
        if depth != "0":
            sub_stmt_select_from = select_collection_or_item.alias()
            select_sub_stmt = (
                sa.select(
                    sub_stmt_select_from.c,
                )
                .select_from(
                    sub_stmt_select_from,
                )
                .where(
                    sa.and_(
                        sub_stmt_select_from.c.parent_id == self_collection._id,
                        sub_stmt_select_from.c.type_ == "collection",
                    ),
                )
            )
            for row in connection.execute(select_sub_stmt):
                path = "/".join(path_parts)
                path += "/"
                path += row.name
                l += [create_collection(self, row.id, path)]
            l += list(self_collection._get_all(connection=connection))
        return l

    def discover(
        self,
        path: str,
        depth: str = "0",
        child_context_manager: Optional[
            radicale.types.Callable[
                [str, Optional[str]], radicale.types.ContextManager[None]
            ]
        ] = None,
        user_groups: Set[str] = set([]),
    ) -> Iterable["radicale.types.CollectionOrItem"]:
        with self._engine.begin() as c:
            return self._discover(path, connection=c, depth=depth)

    def _move(
        self,
        item: "radicale_item.Item",
        to_collection: "BaseCollection",
        to_href: str,
        *,
        connection,
    ) -> None:
        assert isinstance(item.collection, Collection)
        assert isinstance(to_collection, Collection)
        src_collection_id = item.collection._id
        dst_collection_id = to_collection._id
        item_table = self._meta.tables["cas.item"]

        delete_stmt = sa.delete(
            item_table,
        ).where(
            sa.and_(
                item_table.c.collection_id == dst_collection_id,
                item_table.c.name == to_href,
            )
        )
        update_stmt = (
            sa.update(
                item_table,
            )
            .values(
                collection_id=dst_collection_id,
                name=to_href,
            )
            .where(
                sa.and_(
                    item_table.c.collection_id == src_collection_id,
                    item_table.c.name == item.href,
                )
            )
        )
        connection.execute(delete_stmt)
        connection.execute(update_stmt)
        self._collection_updated(src_collection_id, connection=connection)
        self._collection_updated(dst_collection_id, connection=connection)
        to_collection._update_history_etag(to_href, item, connection=connection)
        assert item.href is not None
        item.collection._update_history_etag(item.href, None, connection=connection)

    def move(
        self, item: "radicale_item.Item", to_collection: "BaseCollection", to_href: str
    ) -> None:
        with self._engine.begin() as c:
            return self._move(item, to_collection, to_href, connection=c)

    def _create_collection(
        self,
        href: str,
        *,
        connection,
        items: Optional[Iterable["radicale_item.Item"]] = None,
        props: Optional[Mapping[str, str]] = None,
    ) -> "BaseCollection":
        logger.debug("create_collection: %s, %s, %s", href, items, props)
        path = self._split_path(href)
        parent_id = self._root_collection.id
        collection_table = self._meta.tables["cas.collection"]
        collection_metadata_table = self._meta.tables["cas.collection_metadata"]
        item_table = self._meta.tables["cas.item"]

        collection_tag = None

        if props is not None:
            tag = props.get("tag")
            if tag == "VADDRESSBOOK":
                collection_tag = 0
            elif tag == "VCALENDAR":
                collection_tag = 1

        if len(path) == 2:
            if not is_valid_uuid(path[1]):
                path = [path[0], str(uuid.uuid4())]
        elif len(path) > 2:
            raise ValueError("Invalid path")

        for i, p in enumerate(path):
            select_stmt = (
                sa.select(
                    collection_table.c,
                )
                .select_from(
                    collection_table,
                )
                .where(
                    sa.and_(
                        collection_table.c.parent_id == parent_id,
                        collection_table.c.name == p,
                    ),
                )
            )
            c = connection.execute(select_stmt).one_or_none()
            if c is None:
                insert_stmt = (
                    sa.insert(
                        collection_table,
                    )
                    .values(
                        id=uuid.UUID(path[1]) if i == 1 else uuid.uuid4(),
                        domain_id=1,
                        tag=collection_tag,
                        parent_id=parent_id,
                        name=p,
                    )
                    .returning(
                        collection_table.c,
                    )
                )
                c = connection.execute(insert_stmt).one()
            parent_id = c.id
        if items is not None or props is not None:
            # drop all subcollections and items
            delete_collections_stmt = sa.delete(
                collection_table,
            ).where(
                collection_table.c.parent_id == parent_id,
            )
            delete_meta_stmt = sa.delete(
                collection_metadata_table,
            ).where(
                collection_metadata_table.c.collection_id == parent_id,
            )
            delete_items_stmt = sa.delete(
                item_table,
            ).where(
                item_table.c.collection_id == parent_id,
            )
            connection.execute(delete_collections_stmt)
            connection.execute(delete_meta_stmt)
            connection.execute(delete_items_stmt)
        if props is not None:
            insert_stmt = sa.insert(
                collection_metadata_table,
            ).values(
                [
                    dict(collection_id=parent_id, key=k, value=v)
                    for k, v in props.items()
                ]
            )
            connection.execute(insert_stmt)
        c = Collection(self, parent_id, "/".join(path))
        if props is not None and "tag" in props and items is not None:
            suffix = ".bin"
            if props["tag"] == "VADDRESSBOOK":
                suffix = ".vcf"
            elif props["tag"] == "VCALENDAR":
                suffix = ".ics"
            for i in items:
                c._upload(i.uid + suffix, i, connection=connection)
        return c

    def create_collection(
        self,
        href: str,
        items: Optional[Iterable["radicale_item.Item"]] = None,
        props: Optional[Mapping[str, str]] = None,
    ) -> "BaseCollection":
        with self._engine.begin() as c:
            return self._create_collection(href, connection=c, items=items, props=props)

    @radicale.types.contextmanager
    def acquire_lock(self, mod: str, user: str = "") -> Iterator[None]:
        _ = mod, user
        yield

    def _verify(self, *, connection) -> bool:
        _ = connection
        return True

    def verify(self):
        with self._engine.begin() as c:
            return self._verify(connection=c)

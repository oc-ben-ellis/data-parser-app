from dataclasses import dataclass

from data_parser_core.credentials import CredentialProvider
from data_parser_core.kv_store import KeyValueStore
from data_parser_core.storage import Storage


@dataclass
class ParserConfig:
    """parser configuration container."""

    credential_provider: CredentialProvider
    kv_store: KeyValueStore
    storage: Storage

from __future__ import annotations

from openclaw_crm.backends.airtable_backend import AirtableBackend


class FakeTable:
    def __init__(self, token: str, base_id: str, table_name: str):
        self.token = token
        self.base_id = base_id
        self.table_name = table_name
        self.records = [
            {
                "id": "rec1",
                "fields": {
                    "Client": "Acme",
                    "Contact": "Jane",
                    "Stage": "lead",
                    "Budget": "1000",
                },
            },
            {
                "id": "rec2",
                "fields": {
                    "Client": "Globex",
                    "Contact": "Bob",
                    "Stage": "proposal",
                    "Budget": "5000",
                },
            },
        ]

    def all(self):
        return self.records

    def create(self, fields):
        rec = {"id": f"rec{len(self.records)+1}", "fields": fields}
        self.records.append(rec)
        return rec

    def update(self, rec_id, fields):
        for rec in self.records:
            if rec["id"] == rec_id:
                rec["fields"].update(fields)
                return rec
        raise ValueError("record not found")


def make_backend() -> AirtableBackend:
    backend = AirtableBackend(api_token="tok", base_id="base")
    backend._table_cls = FakeTable
    return backend


def test_read_returns_values_with_headers():
    backend = make_backend()
    result = backend.read("ignored", "Pipeline!A:U")

    assert result.success is True
    values = result.data["values"]
    assert values[0][0] == "Client"
    assert values[1][0] == "Acme"
    assert values[2][0] == "Globex"


def test_append_creates_new_record():
    backend = make_backend()

    result = backend.append(
        "ignored",
        "Pipeline!A:U",
        [["NewCo", "Ann", "upwork", "lead", "2000"]],
    )

    assert result.success is True
    created = result.data["created"]
    assert len(created) == 1
    assert created[0]["fields"]["Client"] == "NewCo"


def test_update_modifies_existing_record():
    backend = make_backend()

    result = backend.update(
        "ignored",
        "Pipeline!A2:U2",
        [["Acme", "Jane", "network", "won", "3000"]],
    )

    assert result.success is True
    updated = result.data["updated"]
    assert updated[0]["fields"]["Stage"] == "won"
    assert updated[0]["fields"]["Budget"] == "3000"


def test_missing_credentials_returns_error():
    backend = AirtableBackend(api_token="", base_id="")
    backend._table_cls = FakeTable

    result = backend.read("ignored", "Pipeline!A:U")
    assert result.success is False
    assert "Missing Airtable API token" in result.error

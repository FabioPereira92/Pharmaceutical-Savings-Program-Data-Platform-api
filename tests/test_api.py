def test_missing_api_key(client):
    r = client.get("/coupon", params={"drug_name": "Eliquis"})
    assert r.status_code == 401
    body = r.json()
    assert body["success"] is False
    assert body["code"] == 401

def test_invalid_api_key(client):
    r = client.get("/coupon", params={"drug_name": "Eliquis"}, headers={"x-api-key": "nope"})
    assert r.status_code == 401

def test_coupon_found(client):
    r = client.get("/coupon", params={"drug_name": "Eliquis"}, headers={"x-api-key": "validkey"})
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert "ai_extraction" in body["data"]
    assert "eliquis" in body["data"]["ai_extraction"].lower()

def test_coupon_not_found(client):
    r = client.get("/coupon", params={"drug_name": "DoesNotExist"}, headers={"x-api-key": "validkey"})
    assert r.status_code == 404
    body = r.json()
    assert body["success"] is False
    assert body["error"]["type"] == "not_found"

def test_coupons_list_filter(client):
    r = client.get("/coupons", params={"drug_name": "eliq"}, headers={"x-api-key": "validkey"})
    assert r.status_code == 200
    body = r.json()
    assert body["data"]["meta"]["drug_name"] == "eliq"
    assert body["data"]["meta"]["total"] >= 1

def test_health_ready(client):
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200

def test_admin_keys(client):
    r = client.get("/admin/keys", headers={"x-admin-key": "adminkey"})
    assert r.status_code == 200
    assert "keys" in r.json()["data"]

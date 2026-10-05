import io
from pathlib import Path
import pytest
from pipeline.publish_s3 import put_immutable, publish
from pipeline.ingest import ingest

boto3 = pytest.importorskip("boto3")
Stubber = pytest.importorskip("botocore.stub").Stubber


def client():
    # Deliberately nonfunctional stub credentials; no network calls are made.
    return boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )


def test_conditional_write_and_collision_contract():
    import hashlib

    c = client()
    stub = Stubber(c)
    params = {
        "Bucket": "portfolio-test",
        "Key": "objects/test",
        "Body": b"example",
        "IfNoneMatch": "*",
        "ServerSideEncryption": "AES256",
        "Metadata": {"sha256": hashlib.sha256(b"example").hexdigest()},
    }
    stub.add_response("put_object", {}, params)
    with stub:
        assert put_immutable(c, "portfolio-test", "objects/test", b"example") == "created"
    stub.assert_no_pending_responses()
    stub = Stubber(c)
    stub.add_client_error(
        "put_object",
        service_error_code="PreconditionFailed",
        http_status_code=412,
        expected_params=params,
    )
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(b"example")},
        {"Bucket": "portfolio-test", "Key": "objects/test"},
    )
    with stub:
        assert put_immutable(c, "portfolio-test", "objects/test", b"example") == "existing"
    stub.assert_no_pending_responses()


def test_manifest_is_last_and_tampering_is_rejected(tmp_path):
    root = Path(__file__).resolve().parents[1]
    report = ingest(root / "data/sample/events.csv", tmp_path)
    calls = []

    class Fake:
        def put_object(self, **kwargs):
            calls.append(kwargs["Key"])

    publish(tmp_path, report["batch_hash"], "portfolio-test", Fake())
    assert calls[-1].startswith("objects/manifests/") and len(calls) == 4
    (tmp_path / report["objects"][1]["key"]).write_text("tampered")
    calls.clear()
    with pytest.raises(ValueError, match="checksum"):
        publish(tmp_path, report["batch_hash"], "portfolio-test", Fake())
    assert not calls

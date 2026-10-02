#!/usr/bin/env python3
# RustFS S3 semantics verification (boto3). Endpoint/creds via env.
import os, sys, json, traceback
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# 凭证一律从环境变量传入，仓库里不落任何真实密钥。
# 103 测试环境的真实取值见 /nfs-shared/hai-platform/override.toml 的 [cloud.storage]。
ENDPOINT = os.environ.get("RFS_ENDPOINT", "http://192.168.100.103:19000")
AK = os.environ.get("RFS_AK", "")
SK = os.environ.get("RFS_SK", "")
if not AK or not SK:
    sys.exit("缺少凭证：请先 export RFS_AK=... RFS_SK=...（见 override.toml 的 [cloud.storage]）")
REGION = "us-east-1"
BP = "hai-platform-private"
BPUB = "hai-platform-public"
KEY = "wsgrp/wstest_a/workspaces/demo/a.txt"
BODY = b"hello-rustfs"          # 12 bytes
TAGS = {"size": "12", "md5": "abc", "source": "cluster", "filemode": "640"}
READONLY = "--readonly" in sys.argv

results = []
def rec(name, ok, detail=""):
    results.append((name, ok, detail))
    print("%s | %s%s" % ("PASS" if ok else "FAIL", name, (" | " + detail) if detail else ""), flush=True)

def mk():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=AK,
        aws_secret_access_key=SK, region_name=REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))

print("=== endpoint=%s ak=%s readonly=%s ===" % (ENDPOINT, AK, READONLY))
c = mk()
cfg = c._request_signer._credentials
print("access_key_id=%s" % cfg.access_key)

# --- 0. connectivity / list_buckets ---
try:
    r = c.list_buckets()
    rec("list_buckets", True, "buckets=" + json.dumps([b["Name"] for b in r.get("Buckets", [])]))
except Exception as e:
    rec("list_buckets", False, repr(e)[:500])

if READONLY:
    print("\n=== READONLY mode: stopping here ===")
    sys.exit(0)

# --- 1. create buckets ---
for b in (BP, BPUB):
    try:
        c.create_bucket(Bucket=b)
        rec("create_bucket:" + b, True, "created")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            rec("create_bucket:" + b, True, "already exists (%s)" % code)
        else:
            rec("create_bucket:" + b, False, repr(e)[:400])
    except Exception as e:
        rec("create_bucket:" + b, False, repr(e)[:400])

# --- 2. put_object ---
try:
    r = c.put_object(Bucket=BP, Key=KEY, Body=BODY, ContentType="text/plain")
    rec("put_object:%s" % KEY, True, "ETag=%s" % r.get("ETag"))
except Exception as e:
    rec("put_object", False, repr(e)[:500])

# --- 3. get_object + assert ---
try:
    r = c.get_object(Bucket=BP, Key=KEY)
    got = r["Body"].read()
    rec("get_object+assert content", got == BODY,
        "got=%r expect=%r len=%d" % (got, BODY, len(got)))
except Exception as e:
    rec("get_object", False, repr(e)[:500])

# --- 4. object tagging (CRITICAL) ---
try:
    c.put_object_tagging(Bucket=BP, Key=KEY,
        Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in TAGS.items()]})
    rec("put_object_tagging", True, "sent=" + json.dumps(TAGS))
except Exception as e:
    rec("put_object_tagging", False, repr(e)[:600])
    print("RAW TRACEBACK:\n" + traceback.format_exc())
try:
    got = {t["Key"]: t["Value"] for t in c.get_object_tagging(Bucket=BP, Key=KEY)["TagSet"]}
    rec("get_object_tagging+assert", got == TAGS, "got=%s expect=%s" % (json.dumps(got), json.dumps(TAGS)))
except Exception as e:
    rec("get_object_tagging", False, repr(e)[:600])
    print("RAW TRACEBACK:\n" + traceback.format_exc())

# --- 5. list_objects_v2 with prefix ---
try:
    r = c.list_objects_v2(Bucket=BP, Prefix="wsgrp/wstest_a/workspaces/demo/")
    keys = [o["Key"] for o in r.get("Contents", [])]
    rec("list_objects_v2 prefix", KEY in keys, "KeyCount=%s keys=%s" % (r.get("KeyCount"), keys))
except Exception as e:
    rec("list_objects_v2", False, repr(e)[:500])

# --- 6. head_object ---
try:
    h = c.head_object(Bucket=BP, Key=KEY)
    rec("head_object", h["ContentLength"] == len(BODY),
        "ContentLength=%s LastModified=%s ETag=%s" % (h["ContentLength"], h["LastModified"], h.get("ETag")))
except Exception as e:
    rec("head_object", False, repr(e)[:500])

# --- 7. delete_object -> list empty ---
try:
    c.delete_object(Bucket=BP, Key=KEY)
    r = c.list_objects_v2(Bucket=BP, Prefix="wsgrp/wstest_a/workspaces/demo/")
    keys = [o["Key"] for o in r.get("Contents", [])]
    rec("delete_object+list empty", len(keys) == 0, "remaining=%s" % keys)
except Exception as e:
    rec("delete_object", False, repr(e)[:500])

# --- 8. multipart upload (2 x 6MiB) ---
MPKEY = "wsgrp/wstest_a/workspaces/demo/multipart.bin"
PART = b"x" * 6 * 1024 * 1024   # 6 MiB
try:
    mp = c.create_multipart_upload(Bucket=BP, Key=MPKEY)
    mid = mp["UploadId"]
    rec("create_multipart_upload", True, "UploadId=%s" % mid)
    parts = []
    for i, data in enumerate((PART, PART), start=1):
        pr = c.upload_part(Bucket=BP, Key=MPKEY, PartNumber=i, UploadId=mid, Body=data)
        parts.append({"ETag": pr["ETag"], "PartNumber": i})
        print("   uploaded part %d: %d bytes ETag=%s" % (i, len(data), pr["ETag"]), flush=True)
    cr = c.complete_multipart_upload(Bucket=BP, Key=MPKEY, UploadId=mid, MultipartUpload={"Parts": parts})
    rec("complete_multipart_upload", True, "ETag=%s Location=%s" % (cr.get("ETag"), cr.get("Location")))
    h = c.head_object(Bucket=BP, Key=MPKEY)
    rec("head_object multipart size", h["ContentLength"] == 2 * len(PART),
        "ContentLength=%s expect=%s" % (h["ContentLength"], 2 * len(PART)))
    c.delete_object(Bucket=BP, Key=MPKEY)
except Exception as e:
    rec("multipart upload", False, repr(e)[:600])
    print("RAW TRACEBACK:\n" + traceback.format_exc())

# --- 9. PROBE: x-amz-tagging header on put_object ---
try:
    c.put_object(Bucket=BP, Key="probe/tagging-header.txt", Body=b"probe-tagging",
                 Tagging="size=6&source=header")
    tg = {t["Key"]: t["Value"] for t in c.get_object_tagging(Bucket=BP, Key="probe/tagging-header.txt")["TagSet"]}
    rec("PROBE x-amz-tagging header on put_object", True, "server-side tags=%s" % json.dumps(tg))
    c.delete_object(Bucket=BP, Key="probe/tagging-header.txt")
except Exception as e:
    rec("PROBE x-amz-tagging header on put_object", False, repr(e)[:600])

# --- 10. PROBE: STS AssumeRole ---
try:
    sts = boto3.client("sts", endpoint_url=ENDPOINT, aws_access_key_id=AK, aws_secret_access_key=SK,
                       region_name=REGION, config=Config(retries={"max_attempts": 1}))
    r = sts.assume_role(RoleArn="arn:aws:iam::000000000000:role/rustfs-test", RoleSessionName="rfstest")
    rec("PROBE sts.assume_role (signed)", True, json.dumps(r.get("Credentials", {}), default=str)[:300])
except Exception as e:
    rec("PROBE sts.assume_role (signed)", False, repr(e)[:700])

try:
    import urllib.request
    u = ENDPOINT + "/?Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fx&RoleSessionName=x"
    try:
        resp = urllib.request.urlopen(u, timeout=8)
        rec("PROBE raw Action=AssumeRole (unsigned)", True, "HTTP %s body=%s" % (resp.status, resp.read()[:300]))
    except urllib.error.HTTPError as he:
        rec("PROBE raw Action=AssumeRole (unsigned)", False, "HTTP %s body=%s" % (he.code, he.read()[:300]))
except Exception as e:
    rec("PROBE raw Action=AssumeRole (unsigned)", False, repr(e)[:400])

# --- summary ---
req = [x for x in results if not x[0].startswith("PROBE")]
print("\n=== SUMMARY: %d checks, %d failed (excluding PROBE) ===" % (len(req), sum(1 for x in req if not x[1])))
for n, ok, d in results:
    print("  %-5s %s" % ("PASS" if ok else "FAIL", n))
sys.exit(0 if all(x[1] for x in req) else 1)

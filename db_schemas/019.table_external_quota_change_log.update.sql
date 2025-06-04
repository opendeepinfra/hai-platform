alter table "external_quota_change_log"
alter column "original_quota" type integer using quota,
alter column "original_quota" set default 0;



SCHEMA = """
    create table "task" ("id" integer not null, "nb_name" varchar(511), "user_name" varchar(255), "code_file" varchar(2047), 
        "workspace" varchar(255), "config_json" jsonb, "group" varchar(2048), "nodes" integer, "assigned_nodes" jsonb,
        "restart_count" integer, "whole_life_state" integer, "first_id" integer, "backend" varchar(255),  "task_type" varchar(255), 
        "queue_status" varchar(255), "notes" text null, "priority" integer, "chain_id" varchar(255),  "stop_code" integer, 
        "suspend_code" integer, "mount_code" integer, "suspend_updated_at" timestamp, "begin_at" timestamp,  "end_at" timestamp, 
        "created_at" timestamp, "worker_status" varchar(255), "created_year" integer, "created_month" integer, "created_day" integer, 
        "created_hour" integer, "pods" jsonb, "created_seconds" integer, "running_seconds" integer, "gpu_seconds" integer, 
        "schedule_zone" varchar(255), "shared_group" varchar(255), "user_role" varchar(255),
        constraint "pri-task_ng-id" primary key ("id")
    );
    create index "idx-task-user_name" on "task" ("user_name");
    create index "idx-task-nb_name" on "task" ("nb_name");
    create index "idx-task-chain_id" on "task" ("chain_id");
    create index "idx-task-first_id" on "task" ("first_id");
    create index "idx-task-suspend_updated_at" on "task" ("suspend_updated_at");
    create index "idx-task-begin_at" on "task" ("begin_at");
    create index "idx-task-end_at" on "task" ("end_at");
    create index "idx-task-created_at" on "task" ("created_at");
    create index "idx-task-chain_id-varchar" on "task"("chain_id" varchar_pattern_ops);
    create index "idx-task-nb_name-varchar" on "task"("nb_name" varchar_pattern_ops);
    create index "idx-task-worker_status" on "task" ("worker_status");

    create table "user" ("user_id" integer, "active" boolean, "user_name" varchar(255), "nick_name" varchar(255), 
        "role" varchar(255), "shared_group" varchar(255), "last_activity" timestamp,  "resource" varchar(255), 
        "quota" bigint, "user_groups" jsonb, "token" varchar(255),
        constraint "pri-user-user_id-resource" primary key ("user_id", "resource")
    );
    create index "idx-user-user_name" on "user" ("user_name");
    create index "idx-user-nick_name" on "user" ("nick_name");
    create index "idx-user-role" on "user" ("role");
    create index "idx-user-shared_group" on "user" ("shared_group");
    create index "idx-user-last_activity" on "user" ("last_activity");
    create index "idx-user-resource" on "user" ("resource");

    create table "resource" ("name" varchar(255), "mars_group" varchar(255), "type" varchar(255), "gpu_num" integer,
        "status" varchar(255), "origin_group" varchar(255), "schedule_zone" varchar(255),  "memory" bigint,
        "nodes" integer, "cpu" integer, "use" varchar(255), "internal_ip" varchar(255), "cluster" varchar(255),
        "group" varchar(255), "working" varchar(255), "working_user" varchar(255), "leaf" varchar(255), "spine" varchar(255),
        constraint "pri-resource-name" primary key ("name")
    );
    create index "idx-resource-name" on "resource" ("name");
    create index "idx-resource-mars_group" on "resource" ("mars_group");
    create index "idx-resource-type" on "resource" ("type");
    create index "idx-resource-status" on "resource" ("status");
    create index "idx-resource-origin_group" on "resource" ("origin_group");
    create index "idx-resource-schedule_zone" on "resource" ("schedule_zone");
    create index "idx-resource-use" on "resource" ("use");
    create index "idx-resource-group" on "resource" ("group");
    create index "idx-resource-working" on "resource" ("working");
    create index "idx-resource-working_user" on "resource" ("working_user");
    create index "idx-resource-leaf" on "resource" ("leaf");
    create index "idx-resource-spine" on "resource" ("spine"); 
"""

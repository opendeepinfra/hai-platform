create table if not exists "user_group" (
    "user_name" varchar(255) not null,
    "group" varchar(255) not null,
    constraint "pri-user_group-user_name-group" primary key ("user_name", "group")
);
create index if not exists "idx-user_group-group" on "user_group" ("group");
comment on table "user_group" is '用户组信息';
comment on column "user_group"."user_name" is '用户名';
comment on column "user_group"."group" is '组名，一个用户可以在多个组';

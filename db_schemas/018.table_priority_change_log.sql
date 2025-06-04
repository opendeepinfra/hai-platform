create table public.priority_change_log
(
	task_id integer default 0,
    dist_priority integer default 0,
    updated_at timestamp not null default current_timestamp
);

create or replace function update_priority_change_log_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_priority_change_log_updated_at before update on "train_image" for each row execute procedure update_priority_change_log_updated_at();

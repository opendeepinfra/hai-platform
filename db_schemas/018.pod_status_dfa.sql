create or replace function update_pod_status()
returns trigger as $$
begin
    if new."status" = 'building' then
        new."begin_at" = current_timestamp;
        new."end_at" = new."begin_at";
    elsif new."status" in ('succeeded', 'failed', 'stopped') then
        new."end_at" = current_timestamp;
    end if;
    if old."status" in ('succeeded', 'failed', 'stopped') then
        raise exception '已到达终态';
    end if;
    if new."status" in ('succeeded', 'failed', 'stopped') then
        return new;
    end if;
    if old."status" in ('succeeded_terminating', 'failed_terminating', 'stopped_terminating') then
        raise exception '已到达terminating态且新状态不是终态';
    end if;
    if new."status" in ('succeeded_terminating', 'failed_terminating', 'stopped_terminating') then
        return new;
    end if;
    if old."status" in ('running') then
        raise exception '已到达running态';
    end if;
    return new;
end;
$$ language 'plpgsql';
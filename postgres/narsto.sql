/* NARSTO data flags */

create table narsto_flags (
  id serial primary key,
  code text unique,
  definition text,
  description text,
  applicability text
);
COPY narsto_flags(code, definition, description, applicability)
  FROM '/home/wmay/data/flag_codes/narsto.csv' csv header;

--
-- pgvector types. Requires CREATE EXTENSION vector.
--
-- Kept out of t_postgres, which is also loaded into databases that do not have pgvector.
--
drop schema if exists t_pgvector cascade;
--
create schema t_pgvector;
--
create table t_pgvector.vector_types (
  id                         int primary key
 ,vector_col                 vector(3)
 ,unbounded_vector_col       vector
 ,halfvec_col                halfvec(3)
 ,sparsevec_col              sparsevec(5)
);
--
insert into t_pgvector.vector_types
values (1, '[0.1,0.2,0.3]', '[1,2,3,4,5]', '[0.1,0.2,0.3]', '{1:0.1,3:0.2}/5')
      -- Extremes: largest and smallest normal float4, a subnormal float4, largest and smallest subnormal half
      ,(2, '[3.4028235e38,-1.1754944e-38,1e-45]', '[-0.000001]', '[65504,-65504,6e-8]', '{}/5')
      -- Values that need all 9 significant digits to round trip
      ,(3, '[0.333333343,0.666666687,16777217]', '[1.17549435e-38,0.1,-0]', '[0.333,1.5,2]', '{5:-0.333333343}/5')
      ,(4, null, null, null, null);
--
-- An embedding sized table of random values.
--
create table t_pgvector.embeddings (
  id                         int primary key
 ,embedding                  vector(1536)
);
--
insert into t_pgvector.embeddings
select g, (select array_agg(((random() - 0.5) * s.SCALE)::real) from generate_series(1,1536), (select g SCALE) s)::vector(1536)
  from generate_series(1,100) g;
--
select id, vector_col, halfvec_col, sparsevec_col from t_pgvector.vector_types order by id;
--

## derive values from measurements

combine_measures = function(obj, site, data_source, m1, m2,
                            start_time, end_time) {
  site_id = nysatmoschem:::get_site_id(obj$con, site)
  q1 = 'select * from combine_measures(?site, ?ds, ?m1, ?m2) where time between ?start and ?end'
  sql1 = DBI::sqlInterpolate(obj$con, q1, site = site_id, ds = data_source,
                             m1 = m1, m2 = m2, start = start_time, end = end_time)
  obj %>%
    tbl(sql(sql1)) %>%
    collect()
}

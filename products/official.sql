with last_access as (
  select
    *
  from
  (
    select
      row_number() OVER (PARTITION BY split(parse_url(td_url, 'PATH'), '/')[2] ORDER BY time DESC) AS rank,
      split(parse_url(td_url, 'PATH'), '/')[2] as id,
      
      --HTMLから取得しているため、念の為、前後空白をtrim
      trim(kd_isbn) as isbn13,
      trim(td_title) as td_title,
      trim(td_description) as td_description,
      trim(kd_releasedate) as kd_releasedate,
      trim(kd_bookinfolabel) as kd_bookinfolabel,
      trim(kd_booktitle) as kd_booktitle,
      trim(kd_catname) as kd_catname,
      trim(kd_productform) as kd_productform,
      trim(kd_authorname) as kd_authorname
    from
      kadokawa_official.test001
    where
          td_host = 'www.kadokawa.co.jp'  
      and parse_url(td_url, 'PATH') rlike '^/product/[0-9]+/'
  ) a
  where
    rank = 1 --window関数はそのままwhere文に記述できないため、一度サブクエリで呼び出してからwhere記述
),
asin as (
  select
    *
  from
  (
    select
      row_number() OVER (PARTITION BY split(parse_url(td_url, 'PATH'), '/')[2] ORDER BY time DESC) AS rank,
      split(parse_url(td_url, 'PATH'), '/')[2] as id,
      case
        when clicks.href rlike '/gp/' then split(parse_url(href, 'PATH'), '/')[3]
        when clicks.href rlike '/dp/' then split(parse_url(href, 'PATH'), '/')[2]
      end as asin
    from
      kadokawa_official.clicks
    where
          td_host = 'www.kadokawa.co.jp'
      and parse_url(td_url, 'PATH') rlike '^/product/[0-9]+/'
      and clicks.href rlike 'amazon'  
  ) a
  where
    rank = 1
)
insert overwrite table products.official_products
select
  last_access.id as id,
  last_access.isbn13,
  asin.asin as paper_asin,
  last_access.td_title, 
  last_access.td_description,
  last_access.kd_releasedate,
  last_access.kd_bookinfolabel,
  last_access.kd_booktitle,
  last_access.kd_catname,
  last_access.kd_productform,
  last_access.kd_authorname
  --,b.id as paper_id タイトル一致では、一つの電子書籍に複数の紙書籍がひも付き、プロダクトのユニーク性を担保できないため、廃止
from
  last_access
  left join asin on last_access.id = asin.id
  --left join last_access b on translate(regexp_replace(last_access.kd_booktitle, '( |　|\\(|（|\\)|）|<|＜|>|＞)', ''), '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ', '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') = translate(regexp_replace(b.kd_booktitle, '( |　|\\(|（|\\)|）|<|＜|>|＞)', ''), '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ', '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') and last_access.kd_productform = '電子書籍' and b.kd_productform != '電子書籍'

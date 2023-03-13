import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode_outer, col}
import org.apache.spark.sql._

object sactions {
def main(args : Array[String]) :Unit ={
  val spark = SparkSession.builder().appName("first").enableHiveSupport().getOrCreate()
  val ofac_path = args(0)
  val uk_path = args(1)
  val dfOFAC = spark.read.json(ofac_path)
  dfOFAC.printSchema()
  val dfGBR = spark.read.json(uk_path)
  dfGBR.printSchema()
  dfOFAC.write.saveAsTable("OFAC")
  dfGBR.write.saveAsTable("GBR")

  val explode_ofac = "addresses,aliases,reported_dates_of_birth"
  val explode_uk = "addresses,aliases,reported_dates_of_birth"

  val expand_uk = Map("addresses" -> Map("country" -> "country","postal_code"->"postal_code","value"->"city") ,"aliases" -> Map("type"->"alias_type","value"->"alias_value"))
  val expand_ofac = Map("addresses" -> Map("country" -> "country","postal_code"->"postal_code","value"->"city") ,"aliases" -> Map("type"->"alias_type","value"->"alias_value"))

  var dfOFAC_exp = dfOFAC
  var dfGBR_exp = dfGBR

  explode_ofac.split(",").foreach(column => dfOFAC_exp = dfOFAC_exp.withColumn(column,explode_outer(col(column))))
  explode_uk.split(",").foreach(column => dfGBR_exp = dfGBR_exp.withColumn(column,explode_outer(col(column))))

  import org.apache.spark.sql.DataFrame


  dfGBR_exp = df_flatten(dfGBR_exp,expand_uk)
  dfOFAC_exp = df_flatten(dfOFAC_exp,expand_ofac)

  dfOFAC_exp.write.saveAsTable("OFAC_expanded")
  dfGBR_exp.write.saveAsTable("GBR_expanded")

  ///////////////////////////////////////////////////// INDIVIDUAL ///////////////////////////////////////////////////////////////

  spark.conf.set("spark.sql.crossJoin.enabled","true")

  val uk_ind = spark.sql("select alias_value UK_alias_value,alias_type UK_alias_type,city UK_city,postal_code UK_postal_code,country UK_country,id UK_id,name UK_name,cast(unix_timestamp(trim(reported_dates_of_birth), 'dd/MM/yyyy') as string) UK_DOB, place_of_birth UK_POB from default.GBR_expanded  where trim(lower(type)) = 'individual'")


  val us_ind = spark.sql("select alias_value USA_alias_value,alias_type USA_alias_type,city USA_city,postal_code USA_postal_code,country USA_country,id USA_id,name USA_name,cast(unix_timestamp(trim(reported_dates_of_birth), 'dd MMM yyyy') as string) USA_DOB, place_of_birth USA_POB from default.OFAC_expanded  where trim(lower(type)) = 'individual'")


  uk_ind.join(us_ind).write.saveAsTable("combined_cross_join")


  val leven_query = """
     |select * from (select *,
      levenshtein(trim(lower(UK_name)),trim(lower(USA_name))) as leven_name,
      100.0 -(100.0 * levenshtein(trim(lower(UK_name)),trim(lower(USA_name))) / greatest(length(trim(USA_name)),length(trim(UK_name)))) as leven_name_percent,
      levenshtein(trim(lower(UK_alias_value)),trim(lower(USA_alias_value))) as leven_alias,
      100.0 -(100.0 * levenshtein(trim(lower(UK_alias_value)),trim(lower(USA_alias_value))) / greatest(length(trim(USA_alias_value)),length(trim(UK_alias_value)))) as leven_alias_percent,
      levenshtein(trim(lower(UK_country)),trim(lower(USA_country))) as leven_country,
      100.0 -(100.0 * levenshtein(trim(lower(UK_country)),trim(lower(USA_country))) / greatest(length(trim(USA_country)),length(trim(UK_country)))) as leven_country_percent,
      levenshtein(trim(lower(UK_city)),trim(lower(USA_city))) as leven_city,
      100.0 -(100.0 * levenshtein(trim(lower(UK_city)),trim(lower(USA_city))) / greatest(length(trim(USA_city)),length(trim(UK_city)))) as leven_city_percent,
      levenshtein(trim(lower(UK_DOB)),trim(lower(USA_DOB))) as leven_DOB,
      100.0 -(100.0 * levenshtein(trim(lower(UK_DOB)),trim(lower(USA_DOB))) / greatest(length(trim(USA_DOB)),length(trim(UK_DOB)))) as leven_DOB_percent
      from combined_cross_join)tbl where leven_name_percent > 80 order by leven_name_percent desc
      """


  spark.sql(leven_query).write.saveAsTable("leven_individual")

  val name_weight = 85
  val DOB_weight = 10
  val alias_weight = 3
  val country_weight = 2
  val weighted_percentage_threshold = 70

  val weight_query = s"""
WITH leven_weighted as
(SELECT  *,((${name_weight} * coalesce(leven_name_percent,0)) + (${DOB_weight} * coalesce(leven_dob_percent,0)) + (${alias_weight} * coalesce(leven_alias_percent,0))) / (${name_weight} + ${DOB_weight} + ${alias_weight}) * 100 / 100 AS weighted_similarity_percentage from leven_individual),
ranked as
(select *, row_number() over (partition by uk_id, usa_id order by weighted_similarity_percentage desc) similarity_ranking from leven_weighted where weighted_similarity_percentage > ${weighted_percentage_threshold}),
rank_filter as
(select * from ranked where similarity_ranking = 1)
select
orig.us_id ofac_id,
orig.uk_id uk_id,
orig.us_name,
orig.uk_name,
orig.us_addresses,
orig.uk_addresses,
orig.us_aliases,
orig.uk_aliases,
orig.us_DOB,
orig.uk_DOB,
orig.us_type,
orig.uk_type,
rnk.leven_name_percent,
rnk.leven_alias_percent,
rnk.leven_dob_percent,
rnk.weighted_similarity_percentage from
(select ofac.id us_id,
gbr.id uk_id,
ofac.name us_name,
gbr.name uk_name,
ofac.aliases us_aliases,
gbr.aliases uk_aliases,
ofac.addresses us_addresses,
gbr.addresses uk_addresses,
ofac.id_numbers us_id_numbers,
gbr.id_numbers uk_id_numbers,
ofac.place_of_birth us_place_of_birth,
gbr.place_of_birth uk_place_of_birth,
ofac.reported_dates_of_birth us_DOB,
gbr.reported_dates_of_birth uk_DOB,
ofac.type us_type,
gbr.type uk_type
from gbr
join ofac
on trim(lower(gbr.type)) ='individual' and trim(lower(ofac.type)) = 'individual') orig
inner join
(select * from rank_filter) rnk
on rnk.usa_id =orig.us_id and rnk.uk_id=orig.uk_id"""

  spark.sql(weight_query).write.mode("overwrite").saveAsTable("weighted_leven_individual")

  ////////////////////////////////ENTITY////////////////////////////////////////////

  val leven_entity="""
WITH
t1 as
(select addresses UK_addresses,aliases UK_aliases,id_numbers UK_id_numbers,id UK_id,name UK_name,reported_dates_of_birth uk_dob, gbr.type uk_type from default.gbr  where trim(lower(type)) = 'entity'),
t2 as
(select addresses US_addresses,aliases US_aliases,id_numbers USA_id_numbers,id ofac_id,name US_name, reported_dates_of_birth us_dob, ofac.type us_type from default.ofac  where trim(lower(type)) = 'entity'),
t3 as
(select * from t1 join t2),
t4 as
(select *,levenshtein(trim(lower(UK_name)),trim(lower(US_name))) as leven_dist, 100.0 -(100.0 * levenshtein(trim(lower(UK_name)),trim(lower(US_name))) / greatest(length(trim(US_name)),length(trim(UK_name)))) as leven_name_percent from t3)
select ofac_id,
uk_id,
us_name,
uk_name,
us_addresses,
uk_addresses,
us_aliases,
uk_aliases,
us_DOB,
uk_DOB,
us_type,
uk_type,
leven_name_percent,
0.0 as leven_alias_percent,
0.0 as leven_dob_percent,
leven_name_percent as weighted_similarity_percentage
from t4 where leven_name_percent > 80"""


  spark.sql(leven_entity).write.mode("overwrite").saveAsTable("weighted_leven_entity")

    //////////////////////////////// UNION ////////////////////////////////////////////

    val union_query = """
select * from weighted_leven_individual
UNION
select * from weighted_leven_entity
"""

    spark.sql(union_query).write.mode("overwrite").saveAsTable("final_result")
  }

    def df_flatten(df :DataFrame , expandMap : Map[String,Map[String,String]]) : DataFrame = {
    var dfRes = df
    expandMap.foreach(column => {
    val col_name = column._1
    val col_map = column._2
    col_map.foreach(elem => {
    val field = elem._1
    val alias = elem._2
    println(s"parent column is $col_name. sub column is $field. Alias is $alias")
    dfRes = dfRes.selectExpr(s"$col_name.$field as $alias", "*")
    })
    dfRes = dfRes.drop(s"$col_name")
  })
    dfRes
}

}



case class CityClickProduct(city_id:Long,
                            click_product_id:Long)

case class CityAreaInfo(city_id:Long,
                        city_name:String,
                        area:String)

//***************** 输出表 *********************

/**
  *
  * @param taskid
  * @param area
  * @param areaLevel
  * @param pid
  * @param cityInfos
  * @param click_count
  * @param product_name
  * @param product_status
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           pid:Long,
                           cityInfos:String,
                           click_count:Long,
                           product_name:String,
                           product_status:String)
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType



object Main {
  System.setProperty("hadoop.home.dir", "C:\\Users\\Anikei\\hadoop-3.3.1")
  def main(args: Array[String]): Unit = {

    //инициализируем спарк сессию
    val spark = SparkSession.builder()
      .appName("final_test_bank")
      .config("spark.executor.memory", "1g")
      .config("spark.driver.memory", "1g")
      .master("local[8]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //выгружаем таблицы из БД
    val dfAccount = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "bank.\"Account\"")
      .option("user", "postgres")
      .option("password", "12345")
      .load()

    val dfClient = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "bank.\"Client\"")
      .option("user", "postgres")
      .option("password", "12345")
      .load()

    val dfOperation = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "bank.\"Operation\"")
      .option("user", "postgres")
      .option("password", "12345")
      .load()

    val dfRate = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "bank.\"Rate\"")
      .option("user", "postgres")
      .option("password", "12345")
      .load()

    val dfList = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "bank.\"List\"")
      .option("user", "postgres")
      .option("password", "12345")
      .load()



    //выбираем актуальные курсы на отчетную дату ( можно в исходных данных поставить 11 месяц и будет чуть интереснее, сейчас там максимальный курс от 01.02)
    val  dfCurRateDate = dfOperation.select(col("DateOp")).distinct()
      .join(dfRate, col("DateOp") >= col("RateDate"),"left")
      .groupBy(col("DateOp"))
      .agg(max(col("RateDate")).as("RateDateActual"))

    val dfCurRate = dfCurRateDate
      .join(dfRate, col("RateDateActual") === col("RateDate"), "inner")
      .select(col("DateOp"),col("RateDateActual"), col("Rate"), col("Currency"))

//подготовка списков масок
    val dfListCorrect = dfList
    .withColumn("ListCorrect",
       regexp_replace(
         regexp_replace(
           col("List"),
            "%",
            ""
         ),
        ", ",
        "|"
       )
    ).select(col("ListCorrect"))


    val listCarsAmt = dfListCorrect.where(col("ListName") === lit("CarsAmt")).select(col("ListCorrect")).first().getString(0)
    val listFoodAmt = dfListCorrect.where(col("ListName") === lit("FoodAmt")).select(col("ListCorrect")).first().getString(0)

// уникальные счета-клиенты на дату

    //дебетовые счета
    val dfAccountDBDistinct = dfOperation
       .select(col("AccountDB").as("AccountId"),col("DateOp"))
      .distinct()
    //дебетовые + клиент
    val dfAccountDBClientDistinct = dfAccountDBDistinct
      .join(dfAccount.as("t2"), dfAccountDBDistinct.col("AccountId") === dfAccount.col("AccountId"))
      .join(dfClient.as("t3"),  col("t2.ClientId") === col("t3.ClientId"))
      .select(dfAccountDBDistinct.col("AccountId"), col("AccountNum"),  dfClient.col("ClientId"),
        col("Type"), col("DateOp"), col("DateOpen"), col("ClientName"))

    // кредитные
    val dfAccountCRDistinct = dfOperation.select(col("AccountCR").as("AccountId"),col("DateOp"))
      .distinct()
    // кредиттные + клиент
    val dfAccountCRClientDistinct = dfAccountCRDistinct
      .join(dfAccount.as("t2"), dfAccountCRDistinct.col("AccountId") === dfAccount.col("AccountId"))
      .join(dfClient.as("t3"), col("t2.ClientId") === col("t3.ClientId"))
      .select(dfAccountCRDistinct.col("AccountId"), col("AccountNum"), dfClient.col("ClientId"),
        col("Type"), col("DateOp"), col("DateOpen"), col("ClientName"))

    //уникальные счета-клиенты из дебета и кредита
    val dfAccClnt = dfAccountDBClientDistinct.union(dfAccountCRClientDistinct)


    // начало построения витрины corporate_payments, обогащение данными о счетах, клиентах

    val dfCorporatePaymentsDBBase = dfAccClnt.as("t1")
      .join(dfOperation.as("t2"), dfOperation("AccountDB") === dfAccountDBClientDistinct("AccountId")
        && col("t1.DateOp") === col("t2.DateOp")
        , "inner")
      .select(col("AccountDB"), col("ClientId").as("ClientIdDB"), col("AccountNum").as("AccountNumDB")
        , col("AccountCR"), col("Amount"), col("Currency"), col("Comment"),
        col("t1.DateOp").as("CutoffDt"))
   // dfCorporatePaymentsDBBase.show(10, false)

    // добавляем данные по счетам кредита
    val dfCorporatePaymentsBaseAddCR = dfCorporatePaymentsDBBase.as("t1")
      .join(dfAccClnt.as("t2"), dfCorporatePaymentsDBBase("AccountCR") === dfAccClnt("AccountId")
        && col("t1.CutoffDt") === col("t2.DateOp")
      )
      .select(col("AccountDB"), col("AccountNumDB"), col("ClientIdDB"),
        col("AccountCR"), col("ClientId").as("ClientIdCR"), col("AccountNum").as("AccountNumcr"),
        col("Amount"), col("Currency"), col("Comment"), col("CutoffDt"),
        col("t2.Type").as("ClientCRType") )

    // пересчет сумм на рубли
    val dfCorporatePaymentsChangeLocAmount = dfCorporatePaymentsBaseAddCR.as("t1")
      .join(dfCurRate.as("t2"),
        col("t1.Currency") === col("t2.Currency") &&
          col("t1.CutoffDt") ===  col("t2.DateOp"), "left")
      .withColumn("localAmount", (col("Amount") * col("Rate")).cast(DecimalType(18,2)))
      .select(col("AccountDB"), col("AccountNumDB"), col("ClientIdDB"),
        col("AccountCR"), col( "ClientIdCR"), col( "AccountNumcr"),
        col("Amount"), col("t1.Currency"), col("Comment"), col("CutoffDt"),
        col( "ClientCRType"), col("localAmount"))
   // dfCorporatePaymentsChangeLocAmount.show(10,false)

    //вычисление агрегатов для дебета
    val dfCorporatePaymentsDB = dfCorporatePaymentsChangeLocAmount
      .groupBy(col("AccountDB").as("AccountId"),col("ClientIdDB").as("ClientId"), col("CutoffDt"))
      .agg(
        sum(
          when(col("AccountDB").isNotNull,
            col("localAmount"))
            .otherwise(0)
        ).cast(DecimalType(18,2)).as("PaymentAmt"),
        sum(
          when(col("AccountDB").isNotNull && col("AccountNumcr").substr(0,5) === lit("40702"),
            col("localAmount"))
            .otherwise(0)
        ).cast(DecimalType(18,2)).as("TaxAmt"),
        sum(
          when(col("AccountDB").isNotNull && col("ClientCRType")  === lit("Ф"),
            col("localAmount"))
            .otherwise(0)
        ).cast(DecimalType(18,2)).as("FLAmt"),
        sum(
          when(  col("Comment").rlike(listCarsAmt) ===  true,
            0)
            .otherwise(col("localAmount"))
        ).cast(DecimalType(18,2)).as("CarsAmt")

      )
   // dfCorporatePaymentsDB.where(col("CutoffDt").isNull).show(10,false)


    //вычисление агрегатов для кредита
    val dfCorporatePaymentsCR = dfCorporatePaymentsChangeLocAmount
      .groupBy(col("AccountCR").as("AccountId"), col("ClientIdCR").as("ClientId"), col("CutoffDt"))
      .agg(
            sum(
              when(col("AccountCR").isNotNull,
                col("localAmount"))
                .otherwise(0)
            ).cast(DecimalType(18,2)).as("EnrollementAmt"),
            sum(
              when(col("AccountCR").isNotNull && col("AccountNumDB").substr(0, 5) === lit("40802"),
                col("localAmount"))
                .otherwise(0)
            ).cast(DecimalType(18,2)).as("ClearAmt"),
        sum(
          when( col("Comment").rlike(listFoodAmt) ===   true,
            col("localAmount"))
            .otherwise(0)
        ).cast(DecimalType(18,2)).as("FoodAmt")
      )
   // dfCorporatePaymentsCR.show(10,false)



    //финальная витрина
    val dfCorporatePayments = dfCorporatePaymentsCR.as("t1")
      .join(dfCorporatePaymentsDB.as("t2"), col("t1.AccountId") ===  col("t2.AccountId") &&
        col("t1.ClientId") ===  col("t2.ClientId") &&
        col("t1.CutoffDt") ===  col("t2.CutoffDt") , "full")
      .select(coalesce(col("t1.AccountId"),col("t2.AccountId")).as("AccountId"),
        coalesce(col("t1.ClientId"),col("t2.ClientId")).as("ClientId"),
        coalesce(col("PaymentAmt"),lit(0)).as("PaymentAmt"),
        coalesce(col("EnrollementAmt"),lit(0)).as("EnrollementAmt"),
        coalesce(col("TaxAmt"),lit(0)).as("TaxAmt"),
        coalesce(col("ClearAmt"),lit(0)).as("ClearAmt"),
        coalesce(col("CarsAmt"),lit(0)).as("CarsAmt"),
        coalesce(col("FoodAmt"),lit(0)).as("FoodAmt"),
        coalesce(col("FLAmt"),lit(0)).as("FLAmt"),
        coalesce(col("t1.CutoffDt"),col("t2.CutoffDt")).as("CutoffDt")
      )
    dfCorporatePayments.write.partitionBy("CutoffDt").mode("overwrite").parquet("corporate_payments")





   // Витрина _corporate_account_. Строится по каждому уникальному счету из таблицы Operation на заданную дату расчета. Ключ партиции CutoffDt

    val dfCorporateAccount = dfCorporatePayments.as("t1")
      .join(dfAccClnt.as("t2"), col("t1.AccountId") === col("t2.AccountId") &&
        col("t1.ClientId") === col("t2.ClientId") &&
        col("t1.CutoffDt") === col("t2.DateOp"), "inner")
      .withColumn("TotalAmt",
                  (coalesce(col("PaymentAmt"), lit(0.0)) +
                  coalesce(col("EnrollementAmt"),  lit(0.0))).cast(DecimalType(18,2))
      )
      .select(col("t1.AccountId"),
        col("AccountNum"),
        col("DateOpen"),
        col("t1.ClientId"),
        col("ClientName"),
        col("TotalAmt"),
        col("t1.CutoffDt")
      )
    dfCorporateAccount.write.partitionBy("CutoffDt").mode("overwrite").parquet("corporate_account")


 //   Витрина _corporate_info_. Строится по каждому уникальному клиенту из таблицы Operation. Ключ партиции CutoffDt
    val dfCorporateInfo = dfClient.as("t1")
      .join(dfCorporateAccount.as("t2"), col("t1.ClientId") === col("t2.ClientId"), "inner" )
      .groupBy(col("t1.ClientId"), col("t1.ClientName"), col("t1.Type"), col("t1.Form"),
        col("t1.RegisterDate"),  col("CutoffDt")
      )
      .agg(sum("TotalAmt").cast(DecimalType(18,2)).as("TotalAmt"))
    dfCorporateInfo.write.partitionBy("CutoffDt").mode("overwrite").parquet("corporate_info")
 
  }
}
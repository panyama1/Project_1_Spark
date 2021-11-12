import scala.io.StdIn.readLine
import scala.io.StdIn.readInt
import scala.io.Source
import java.io._


import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object main {
  def main(args: Array[String]):Unit ={


    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    /*

    spark.sql("create table IF NOT EXISTS table(date String,eps double) row format delimited fields terminated by ','");

    spark.sql("LOAD DATA LOCAL INPATH '/Users/arjunpanyam/Downloads/PROJECT1_PANYAM/input/input.csv' INTO TABLE table")
    //spark.sql("SELECT * FROM table").show()
    spark.sql("SELECT * FROM table").show()

     */

    /*
    Earnings Statement
     */


    spark.sql("drop table earnings");
    spark.sql("create table IF NOT EXISTS earnings(ticker String,fiscalDateEnding String,reportedEPS double) row format delimited fields terminated by ','");

    spark.sql("LOAD DATA LOCAL INPATH '/Users/arjunpanyam/Downloads/PROJECT1_PANYAM/input/earnings.csv' INTO TABLE earnings")
    //spark.sql("SELECT * FROM table").show()
    //spark.sql("SELECT * FROM earnings where ticker = 'DHR'").show()

    /*
    Balance Sheet
    */

    spark.sql("drop table balance_sheet");
    spark.sql("create table IF NOT EXISTS balance_sheet(ticker String,fiscalDateEnding String,reportedCurrency String,totalAssets Long,totalCurrentAssets Long,cashAndCashEquivalentsAtCarryingValue Long,cashAndShortTermInvestments Long,inventory Long,currentNetReceivables Long,totalNonCurrentAssets Long,propertyPlantEquipment Long,accumulatedDepreciationAmortizationPPE Long,intangibleAssets Long,intangibleAssetsExcludingGoodwill Long,goodwill Long,investments Long,longTermInvestments Long,shortTermInvestments Long,otherCurrentAssets Long,otherNonCurrrentAssets Long,totalLiabilities Long,totalCurrentLiabilities Long,currentAccountsPayable Long,deferredRevenue Long,currentDebt Long,shortTermDebt Long,totalNonCurrentLiabilities Long,capitalLeaseObligations Long,longTermDebt Long,currentLongTermDebt Long,longTermDebtNoncurrent Long,shortLongTermDebtTotal Long,otherCurrentLiabilities Long,otherNonCurrentLiabilities Long,totalShareholderEquity Long,treasuryStock Long,retainedEarnings Long,commonStock Long,commonStockSharesOutstanding Long) row format delimited fields terminated by ','");

    spark.sql("LOAD DATA LOCAL INPATH '/Users/arjunpanyam/Downloads/PROJECT1_PANYAM/input/balance_sheet.csv' INTO TABLE balance_sheet")
    spark.sql("SELECT * FROM balance_sheet").show()
    //spark.sql("SELECT * FROM earnings where ticker = 'DHR'").show()

    /*
    Cash Flow Statement
     */

    spark.sql("drop table cash_flow");
    spark.sql("create table IF NOT EXISTS cash_flow(ticker String,fiscalDateEnding String,reportedCurrency String,operatingCashflow Long,paymentsForOperatingActivities Long,proceedsFromOperatingActivities Long,changeInOperatingLiabilities Long,changeInOperatingAssets Long,depreciationDepletionAndAmortization Long,capitalExpenditures Long,changeInReceivables Long,changeInInventory Long,profitLoss Long,cashflowFromInvestment Long,cashflowFromFinancing Long,proceedsFromRepaymentsOfShortTermDebt Long,paymentsForRepurchaseOfCommonStock Long,paymentsForRepurchaseOfEquity Long,paymentsForRepurchaseOfPreferredStock Long,dividendPayout Long,dividendPayoutCommonStock Long,dividendPayoutPreferredStock Long,proceedsFromIssuanceOfCommonStock Long,proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet Long,proceedsFromIssuanceOfPreferredStock Long,proceedsFromRepurchaseOfEquity Long,proceedsFromSaleOfTreasuryStock Long,changeInCashAndCashEquivalents Long,changeInExchangeRate Long,netIncome Long) row format delimited fields terminated by ','");

    spark.sql("LOAD DATA LOCAL INPATH '/Users/arjunpanyam/Downloads/PROJECT1_PANYAM/input/cash_flow.csv' INTO TABLE cash_flow")
    spark.sql("SELECT * FROM cash_flow").show()
    //spark.sql("SELECT * FROM earnings where ticker = 'DHR'").show()



     /*
    1. EPS
     */

    /*
    2. Retained earnings
     */

    /*
    3. operatingCashflow
     */

    /*
    4. Dividend history
     */

    /*
    5. Price history
     */

    /*
    6. Total Assets
     */

    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    println("Welcome to the Stock Application.")
    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    println("")

    var true_password_basic = Source.fromFile("password_basic.txt").getLines().toList
    var true_password_admin = Source.fromFile("password_admin.txt").getLines().toList

    //BASIC
    //ADMIN
    println("Username:")
    var username = readLine()
    println("Password:")
    var password = readLine()

    //if(username.toUpperCase() == "BASIC" || username.toUpperCase() == "ADMIN") {
    if(username.toUpperCase() == "BASIC" && password == true_password_basic(0)) {
      //println("SUCCESS!")
      println("MAIN MENU")
      println("****************")
      println("1. Query Data")
      println("2. Change password")

      var choice_main_menu = readInt()

      if(choice_main_menu == 1) {
        //println("1 was selected")
        println("1. EPS")
        println("2. Retained Earnings")
        println("3. Cash Flow From Operations")
        println("4. Dividend History")
        println("5. Price History")
        println("6. Total Assets")
        println("7. Future Retained Earnings Prediction")

        val choice = readInt()

        if(choice == 1) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT * FROM earnings where ticker = '" + ticker + "'").show()
        }

        // retainedEarnings

        if(choice == 2) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, retainedEarnings FROM balance_sheet where ticker = '" + ticker + "'").show()
        }
        // operatingCashflow
        if(choice == 3) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, operatingCashflow FROM cash_flow where ticker = '" + ticker + "'").show()
        }

        // dividendPayout (Cash Flow Statement)
        if(choice == 4) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, dividendPayout FROM cash_flow where ticker = '" + ticker + "'").show()
        }

        // totalCurrentLiabilities (Balance Sheet)
        if(choice == 5) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, totalCurrentLiabilities FROM balance_sheet where ticker = '" + ticker + "'").show()
        }

        // totalAssets (Balance Sheet)
        if(choice == 6) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, totalAssets FROM balance_sheet where ticker = '" + ticker + "'").show()
        }

        // Retained Earnings Prediction
        if(choice == 7) {
          println("Enter number of years in the future:")
          val f_year = readInt()
          println("Enter ticker:")
          val ticker = readLine()

          val retained_Earnings = 0



          // Formula: (Most Recent Retained Earnings) + (Most Recent Net Income - Most Recent Divdend Payout) * number of years)
          spark.sql("SELECT retainedEarnings + ((netIncome - dividendPayout) * " + f_year + ") FROM balance_sheet, cash_flow WHERE balance_sheet.ticker = '" + ticker + "' AND balance_sheet.ticker = cash_flow.ticker AND balance_sheet.fiscalDateEnding = cash_flow.fiscalDateEnding LIMIT 1").show()
          //spark.sql("SELECT ticker, fiscalDateEnding, operatingCashflow FROM cash_flow where ticker = '" + ticker + "'").show()
        }





      } else if (choice_main_menu == 2) {
        //println("2 was selected")
        println("Enter new password:")
        var new_password_basic = readLine()
        //true_password_basic = new_password_basic

        val writer = new PrintWriter(new File("password_basic.txt"))
        writer.write(new_password_basic)
        writer.close()

        println("Password changed")

      } else {
        println("ERROR! Enter 1 or 2!")
      }
    } else if(username.toUpperCase() == "ADMIN" && password == true_password_admin(0)) {
      //println("SUCCESS!")
      println("MAIN MENU")
      println("****************")
      println("1. Query Data")
      println("2. Change password")

      var choice_main_menu = readInt()

      if(choice_main_menu == 1) {
        //println("1 was selected")
        println("1. EPS")
        println("2. Retained Earnings")
        println("3. Cash Flow From Operations")
        println("4. Dividend History")
        println("5. Price History")
        println("6. Total Assets")
        println("7. Future Retained Earnings Prediction")

        val choice = readInt()

        if(choice == 1) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT * FROM earnings where ticker = '" + ticker + "'").show()
        }

        // retainedEarnings

        if(choice == 2) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, retainedEarnings FROM balance_sheet where ticker = '" + ticker + "'").show()
        }
        // operatingCashflow
        if(choice == 3) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, operatingCashflow FROM cash_flow where ticker = '" + ticker + "'").show()
        }

        // dividendPayout (Cash Flow Statement)
        if(choice == 4) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, dividendPayout FROM cash_flow where ticker = '" + ticker + "'").show()
        }

        // totalCurrentLiabilities (Balance Sheet)
        if(choice == 5) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, totalCurrentLiabilities FROM balance_sheet where ticker = '" + ticker + "'").show()
        }

        // totalAssets (Balance Sheet)
        if(choice == 6) {
          println("Enter ticker:")
          val ticker = readLine()

          spark.sql("SELECT ticker, fiscalDateEnding, totalAssets FROM balance_sheet where ticker = '" + ticker + "'").show()
        }

        // Retained Earnings Prediction
        if(choice == 7) {
          println("Enter number of years in the future:")
          val f_year = readInt()
          println("Enter ticker:")
          val ticker = readLine()

          val retained_Earnings = 0



          // Formula: (Most Recent Retained Earnings) + (Most Recent Net Income - Most Recent Divdend Payout) * number of years)
          spark.sql("SELECT retainedEarnings + ((netIncome - dividendPayout) * " + f_year + ") FROM balance_sheet, cash_flow WHERE balance_sheet.ticker = '" + ticker + "' AND balance_sheet.ticker = cash_flow.ticker AND balance_sheet.fiscalDateEnding = cash_flow.fiscalDateEnding LIMIT 1").show()
          //spark.sql("SELECT ticker, fiscalDateEnding, operatingCashflow FROM cash_flow where ticker = '" + ticker + "'").show()
        }

      } else if (choice_main_menu == 2) {
        //println("2 was selected")
        println("Enter new password:")
        var new_password_basic = readLine()
        //true_password_basic = new_password_basic

        val writer = new PrintWriter(new File("password_basic.txt"))
        writer.write(new_password_basic)
        writer.close()

        println("Password changed")

      } else {
        println("ERROR! Enter 1 or 2!")
      }
    } else {
      println("ERROR! INVALID CREDENTIALS")
    }
  }
}
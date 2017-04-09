import scala.language.postfixOps


object Initializer extends App {

  val keyspace = "assignment"
  val perform  = new operations(keyspace)


  if(perform.ConnectToCassandra){

    perform.createAndPopulateUserTable
    perform.createAndPopulateVideoTables

    val res = perform selectUserByEmail "a@b.com"
    val iterate = res.iterator()
    while (iterate.hasNext) {
      println(iterate.next())
    }

    val res2 = perform selectVideoByVideoName "video1"
    val iterate2 = res2.iterator()
    while (iterate2.hasNext) {
      println(iterate2.next())
    }

    val res3 = perform selectVideoReleasedAfter2015
    val iterate3 = res3.iterator()
    while (iterate3.hasNext) {
      println(iterate3.next())
    }

    val res4 = perform selectVideoByUserId 1
    val iterate4 = res4.iterator()
    while (iterate4.hasNext) {
      println(iterate4.next())
    }

  }
  else {
     println("MESSAGE: Provided Keyspace already exists")
  }



}

import com.datastax.driver.core.{Cluster, ResultSet, Session}
import scala.util.{Failure, Success, Try}

class operations(keyspace:String) {

  def generateSession(keyspace:Option[String]):Session={
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    keyspace match {
      case Some(x) => cluster.connect(x)
      case None => cluster.connect()
    }
  }

  def ConnectToCassandra:Boolean ={
    val session = generateSession(None)
    Try{
      session.execute(s"create keyspace $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.close()
      true
    }
    match{
      case Success(data) => data
      case Failure(msg)  => session.close(); println(msg); false
    }
  }


  def createAndPopulateUserTable:Boolean ={
    val session = generateSession(Some(keyspace))
    Try{
      session.execute("CREATE TABLE user(email text PRIMARY KEY,password text,userId int)")
      session.execute("INSERT INTO user(email,password,userId) VALUES ('a@bz.com','qwertyuiop',1)")
      session.execute("INSERT INTO user(email,password,userId) VALUES ('abc@xyz.com','qwertyuiop',2)")
      session.execute("INSERT INTO user(email,password,userId) VALUES ('a@b.com','qwertyuiop',3)")
      session.close()
      true
    }
    match{
      case Success(data) =>data
      case Failure(msg)  =>println(msg);session.close();false
    }

  }

  def createAndPopulateVideoTables:Boolean ={
    val session = generateSession(Some(keyspace))
    Try{
      session.execute("CREATE TABLE videoByName(videoId int,videoName text,userId int,year int,PRIMARY KEY (videoName,videoId))")
      session.execute("CREATE TABLE videoByYear(videoId int,videoName text,userId int,year int,PRIMARY KEY (userId,year))")
      session.execute("CREATE TABLE videoByUserId(videoId int,videoName text,userId int,year int,PRIMARY KEY (userId,year))")
      InsertDataToTable("videoByName")
      InsertDataToTable("videoByYear")
      InsertDataToTable("videoByUserId")
      session.close()
      true
    }
    match{
      case Success(data) =>data
      case Failure(msg)  =>println(msg);session.close();false
    }

  }

  def InsertDataToTable(tableName:String):Boolean ={
    val session = generateSession(Some(keyspace))
    Try{
      session.execute(s"INSERT INTO $tableName(videoId,videoName,userId,year)VALUES(1,'video1',1,2014)")
      session.execute(s"INSERT INTO $tableName(videoId,videoName,userId,year)VALUES(2,'video2',2,2018)")
      session.execute(s"INSERT INTO $tableName(videoId,videoName,userId,year)VALUES(3,'video3',3,2017)")
      session.close()
      true
    }
    match{
      case Success(data) =>data
      case Failure(msg)  =>println(msg);session.close();false
    }
  }


  def selectUserByEmail(email:String):ResultSet={
    val session = generateSession(Some(keyspace))
    val res = session.execute(s"SELECT * FROM user where email='$email'")
   session.close()
   res
  }

  def selectVideoByVideoName(name:String):ResultSet={
    val session = generateSession(Some(keyspace))
    val res = session.execute(s"SELECT * FROM videoByName where videoname='$name'")
    session.close()
    res
  }

  def selectVideoReleasedAfter2015:ResultSet={
    val session = generateSession(Some(keyspace))
    val res = session.execute(s"SELECT * FROM videoByYear where userid = 2 and year > 2015")
    session.close()
    res
  }
  def selectVideoByUserId(userId:Int):ResultSet={
    val session = generateSession(Some(keyspace))
    val res = session.execute(s"SELECT * FROM videoByUserId where userId = $userId ORDER BY year DESC")
    session.close()
    res
  }



}

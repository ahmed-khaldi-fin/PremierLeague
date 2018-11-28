import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Encoders, SparkSession, functions}

case class Team(team: String, wins: Double, losses: Double, goals: Double, totalYelCard: Double,
                totalRedCard: Double, totalScoAtt: Double,
                onTargetScoAtt: Double, hitWoodwork: Double, attHdGoal: Double, attPenGoal: Double,
                attFreeKickGoal: Double, attIboxGoal: Double, attOboxGaol: Double, goalFastbreak: Double,
                totalOffside: Double, cleanSheet: Double, goalsConceded: Double,
                saves: Double, outfielderBlock: Double, interception: Double, totalTackle: Double,
                lastManTackle: Double, totalClearance: Double, headClearance: Double, ownGoals: Double,
                penConceded: Double, penGoalsConceded: Double,
                totalPass: Double, totalThroughBall: Double, totalLongBalls: Double, backwardPass: Double,
                totalCross: Double,
                cornerTaken: Double, touches: Double, bigChancesMissed: Double, clearanceOffLine: Double,
                dispossessed: Double, penaltySave: Double,
                totalHighClaim: Double, punches: Double, season: String) {

}

object PremierLeagueStats extends App {
  def occurenceString(phrase: String, occ: String): Int = {
    StringUtils.countMatches(phrase, occ)
  }

  val path = ConfigFactory.load().getString("path.csv")
  val spark = SparkSession.builder.appName("PremierLeagueStats").master("local").getOrCreate()

  import spark.implicits._

  val schema = Encoders.product[Team].schema
  val csvFile = spark.read.option("header", "true").option("inferSchema", "true").schema(schema).csv(path).as[Team]

  val mod = csvFile.map(team => (team.team, occurenceString(team.team, "er")))

  val goalsPerSeason = csvFile.map(team => (team.season, team.goals, team.team))
  val goalsMax = goalsPerSeason.groupBy("_1" ).agg(functions.max("_2") as "maxGoalsCards").sort("_1")
  val maxGoalsPerSeason = goalsMax.join(goalsPerSeason,Seq("_1")).where($"maxGoalsCards"===$"_2")
  maxGoalsPerSeason.show(10)

  val totalScoringAttempts = csvFile.map(team => (team.season, team.totalScoAtt, team.team))
  val totalScoringAttemptsMax = totalScoringAttempts.groupBy("_1" ).agg(functions.max("_2") as "maxScoAtt").sort("_1")
  val maxTotalScoringAttempts = totalScoringAttemptsMax.join(totalScoringAttempts,Seq("_1")).where($"maxScoAtt"===$"_2")
  maxGoalsPerSeason.show(10)

  val crossOver = maxTotalScoringAttempts.intersect(maxGoalsPerSeason)
  crossOver.show(10)

}

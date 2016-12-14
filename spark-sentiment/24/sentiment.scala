import scala.io.{ Source, Codec }
import scala.collection

  val defaultFile = "/AFINN/AFINN-111.txt"

  val in = getClass.getResourceAsStream(defaultFile)

  val alphaRegex = "[^a-zA-Z\\s]".r
  val redundantWhitespaceRegex = "[\\s]{2,}".r
  val whitespaceRegex = "\\s".r  

  val words = collection.mutable.Map[String, Int]().withDefaultValue(0)

  for (line <- Source.fromInputStream(in).getLines()) {
    val parsed = line.split("\\t")
    words += (parsed(0) -> parsed(1).toInt)
  }
 

  def tokenize(phrase: String): Array[String] = {
    var normalized = alphaRegex.replaceAllIn(phrase, "")
    normalized = redundantWhitespaceRegex.replaceAllIn(normalized, " ")
    normalized = normalized.toLowerCase
    whitespaceRegex.split(normalized)
  }

  def sentiment(wordsList: Array[String]): Float = {
    var length : Int = wordsList.length
    var score : Int = 0
    for (word <- wordsList) {
      var valence : Int = words(word);
      score += valence
    }
    var retval : Float = (score.toFloat / length)
    return retval
  }

val options = Map(
  "collection" -> "enron",
  "zkhost" -> "localhost:9983",
  "query" -> "content_txt:*FERC*",
  "fields" -> "subject,Message_From,Message_To,content_txt"
)
val dfr = sqlContext.read.format("solr");
val df = dfr.options(options).load;

val peopleEmails = collection.mutable.Map[String, Int]().withDefaultValue(0)
val peopleAfins = collection.mutable.Map[String, Float]().withDefaultValue(0)

 def peoplesEmails(email: String, sentiment: Float) = {
  var peopleEmail: Int = peopleEmails(email);
  var peopleAfin: Float = peopleAfins(email);
  peopleEmail += 1;
  peopleAfin += sentiment;
  peopleEmails.put(email, peopleEmail);
  peopleAfins.put(email, peopleAfin);
 }

 def normalize(email: String): Float = {
   var score: Float = peopleAfins(email);
   var mails: Int = peopleEmails(email);
   var retVal : Float = score / mails
   return retVal
 }

df.collect().foreach( t => peoplesEmails(t.getString(1), sentiment(tokenize(t.getString(3)))
                                        )
                    )

for ((k,v) <- peopleEmails) println( ""+ k + ":" +  normalize(k))

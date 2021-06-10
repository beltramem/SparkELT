import org.apache
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import java.util.Properties
import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar




object SimpleApp extends Serializable {

	// cette fonction nous permet de parcourir tout les sous dossiers et de répertorier tout les fichiers qui s'y trouve
	// renvoie un tableauy contenant les fichiers contenu dans les sous dossiers
	def recursiveListFiles(f: File): Array[File] =
	{

		val these = f.listFiles
		these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
	}

	// cette fonction permet de découper le nom du fichier acq pour récupérer l'identifiant du boiter qui sert d'identifiant au capteur dans la base
	// renvoie l'identifiant sous frome de string
	//on doit obligatoirement utiliser le nom du fichier car l'identifiant capteur ne se trouve pas dans le fichier
	// ATTENTION !!!!!!!!!!!!!!!!!! cette fonction ne fonctionne qu'avec le nommage actuelle des fichiers
	def getCapteurBoitier(file : String): String =
	{
		//on coupe le nom du fichier juste avant le mot "le" qui se trouve avant la date et on ne garde que la première partie
		val split = file.split("le ").head
		//on coupe le nom du fichier à chaque espace (%20) et on prend le dernier mot qui est l'identifiant du boitier
		val boitier = split.split(" ").last
		return boitier
	}


	// Cette fonction permet de récupérer le type du capteur depuis la base de donnée
	def getCapteurType(statement: Statement, idCapteur : String): String =
	{
		var typeCapteur = ""
		// resultset est l'objet contenant les réponses de la requête
		val resultSet = statement.executeQuery(s"""(select type from capteur where id_capteur='${idCapteur}' ) """)
		// il y a maximum un tuple dans la base mais nous devons utiliser while dans le cas où il n' y pas de tuple pour éviter les erreurs
		while (resultSet.next())
			{
				typeCapteur = resultSet.getString("type")
			}
		typeCapteur

	}

	// Cette fonction nous permet de calculer la température à partir de la données brut.
	// le facteur n'est pas optionnel
	def calculTemperature(brut: Double, fct : Double): Double =
	{
		val temperature = 0.01 * brut - fct
		  temperature
	}

	def calculTemperatureMoyenne(date: Timestamp, logement: String): Double =
	{
		val df_temp = spark.sql("select * from mesure_temp")
		//df_temp.show(1)
		val temperature = df_temp.select("temperature").first().getDouble(0)
		temperature
	}


	def calculHr(brut: Double, temperature: Double ): Double =
	{
		val hr = (temperature-25)*(0.01+(0.00008*brut))+(0.0405*brut)+((-0.0000028)*(brut*brut))-4
		hr
	}

	def calculHrMoyen(capteur: String,statement: Statement): Double =
	{
		val rs= statement.executeQuery("select avg(hr) from mesure me join capteur cp on me.capteur=cp.id_capteur where reel = true group by extract(doy from date),logement")
		rs.first()
		val hr = rs.getDouble(1)
		return hr
	}


	def calculDebitPosition1(brut: Double,fct : Double, piece : String): Double =
	{
		var dp1=0.0
		if(!piece.contains("WC")) {
			dp1 = (brut / 1023) * 2.5 - fct
		}
		dp1
	}


	def calculDebitPression1(brut : Double): Double =
	{
		val dp1 = (brut/1023)*4.5
		dp1
	}

	def calculCo2(brut: Double, ty:String): Double =
	{
		var co2 = 0.0
		if(ty=="EA" || ty=="Station") {
			co2 = (brut * 2000) / 818
		}
		co2
	}


	def calculDebitPosition2(brut: Double, Dppoint: Double, hr: Double, rs: ResultSet,reel: Boolean, piece : String): Double =
	{

		var dp2 = 0.0
		if(!piece.contains("WC")) {
			while (rs.next()) {
				if (reel == true) {
					if (Dppoint >= rs.getDouble("borne_inf") && Dppoint < rs.getDouble(("borne_sup"))) {
						if (rs.getDouble("valeur_defaut") <= 0) {
							val fct1 = rs.getDouble("fct1")
							val fct2 = rs.getDouble("fct2")
							val fct3 = rs.getDouble("fct3")
							val fct4 = rs.getDouble("fct4")
							dp2 = fct1 + fct2 * brut + fct3 * math.pow(brut, 2) + fct4 * math.pow(brut, 3)
						}
						else {
							//println("============================================================================ else hr:" + hr)
							dp2 = rs.getDouble("valeur_defaut")
						}
					}
				}
				else
					{
						if (hr >= rs.getDouble("borne_inf") && hr < rs.getDouble(("borne_sup"))) {
							if (rs.getDouble("valeur_defaut") <= 0) {
								val fct1 = rs.getDouble("fct1")
								val fct2 = rs.getDouble("fct2")
								dp2 = fct1 * hr + fct2
							}
							else {
								//println("============================================================================ else hr:" + hr)
								dp2 = rs.getDouble("valeur_defaut")
							}

						}
				}
			}
		}
			dp2
	}

	def calculDebitPression2(Dppoint : Double,dpr1 : Double,rs : ResultSet, ty : String) : Double=
	{
		var dp2=0.0
		if(ty=="Bouche")
			{
		while(rs.next())
		{
			if(Dppoint>=rs.getDouble("borne_inf") && Dppoint<rs.getDouble(("borne_sup")))
			{
				val fct1 = rs.getDouble("fct1")
				dp2 = rs.getDouble("fct1")+rs.getDouble("fct2")*dpr1+rs.getDouble("fct3")*(dpr1*dpr1)
			}
		}}
		dp2
	}

	def calculDebitProduit(dpr2: Double, dp2 : Double, capteurType : String,piece: String): Double=
	{
		var dpro =0.0
		if(capteurType=="Bouche" && !piece.contains("WC"))
		{
			dpro = (1+dpr2)*dp2
		}
		else
			{
				dpro= dp2
			}
		dpro
	}

	def calculHumiditeAbsolue(hr:Double, temperature: Double): Double =
	{
		//println("========================================= humidite absolue hr : temp  "+hr+":"+temperature)
		val ha = (hr/100)*math.exp(18.8161-4110.34/(temperature+235))
		//println("========================================= ha: "+ha)
		ha
	}

	def calculSEQ(dpro: Double, typeCapteur : String): Double=
		{
			var seq = 0.0
			if (typeCapteur == "EA")
				{
					seq = dpro/Math.sqrt(2)
				}
			seq
		}

	def calculDebitReelle(seq: Double,pression_relle:Double): Double =
	{
		seq * math.sqrt(pression_relle/10)

	}

	def getConnection(url:String,connectionProperties:Properties): Statement =
	{
		val connection = DriverManager.getConnection(url,connectionProperties)
		val statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,	ResultSet.CONCUR_READ_ONLY,ResultSet.HOLD_CURSORS_OVER_COMMIT)

	statement
	}

	def closeConnection(statement: Statement): Unit =
	{

		val connection = statement.getConnection
		statement.close()
		connection.close()
	}

	def calculOrientation(brut:Double ): Double=
	{
		val orientation = (brut/1023)*360
		orientation
	}

	def calculVitesseVent(brut:Double): Double=
	{
		val vitesseVent = (((brut/1023)*2.1)*207.5/2.1)+2.5
		vitesseVent
	}

	def orientationBousole(orientation:Double):String=
	{
		var bousole = ""
		if(orientation<=22.5 || orientation>337.5)
			{
				bousole="Nord"
			}
		else if (orientation>=22.5 && orientation<67.5)
			{
				bousole="Nord Est"
			}
		else if (orientation>=67.5 && orientation<112.5)
			{
				bousole="Est"
			}
			else if (orientation>=112.5 && orientation<157.5)
			{
				bousole="Sud Est"
			}
			else if (orientation>=157.5 && orientation<202.5)
			{
				bousole="Sud"
			}
			else if (orientation>=202.5 && orientation<247.5)
			{
				bousole="Sud Ouest"
			}
			else if (orientation>=247.5 && orientation<292.5)
			{
				bousole="Ouest"
			}
			else if (orientation>=292.5 && orientation<337.5)
			{
				bousole="Nord Ouest"
			}

			bousole
	}

	def calculPuissance(brut: Double): Double =
	{
		val puissance = 1.1122*brut-5.4133
		puissance
	}

	def calculPression(brut: Double): Double =
	{
		val pression = 0.7177*brut-43.485
		pression
	}

	val spark = SparkSession.builder.appName("ELT").master("local[4]").getOrCreate()

	def main(args: Array[String]){

		import spark.implicits._
		def supprErreur(mesureErreur : DataFrame, colname: String): DataFrame =
		{
			//retrait des erreurs de température
			val w = apache.spark.sql.expressions.Window.partitionBy().orderBy("date")
			//colonne contenant la valeur précédente
			var mesure = mesureErreur.withColumn("prev_temp", lag(colname,1).over(w))
			//remplacement des valeurs erronée si elle ne sont pas consécutives
			mesure = mesure.withColumn("temp_adjust", when(col(colname).equalTo(61440),col("prev_temp")).otherwise(col(colname)))
			//on ajoute une colonnes contenant null à chaque fois que la température et la température ajustée sont érronnée (il reste une erreur après le premier traitement)
			mesure = mesure.withColumn("zeroNonZero", when((col(colname).equalTo(61440))&&(col("temp_adjust").equalTo(61440)),lit(null)).otherwise(col("temp_adjust")))
			//on remplace chaque valeur null dans la colonne zeroNonZero par la première valeur non null précédente puis on remplace les valeurs de température par celle de la colonne zeroNonZero
			mesure =mesure.withColumn(colname, last("zeroNonZero", ignoreNulls = true).over(w.orderBy("date").rowsBetween(Window.unboundedPreceding, 0))).drop("prev_temp").drop("temp_adjust").drop("zeroNonZero")

			mesure

		}


		val schmBrut = StructType(
			StructField("date", StringType, nullable = false) ::
				StructField("temperature", DoubleType, nullable = false) ::
				StructField("hr", DoubleType, nullable = false) ::
				StructField("debit_position_1", DoubleType, nullable = false) ::
				StructField("debit_pression_1", DoubleType, nullable = false) ::
				StructField("co2", DoubleType, nullable = false) ::
				StructField("capteur", StringType, nullable= true):: Nil)

		val schmBrutCaisson = StructType(
			StructField("Date", StringType, nullable = false) ::
				StructField("vide1", DoubleType, nullable = false) ::
				StructField("vide2", DoubleType, nullable = false) ::
				StructField("vide3", DoubleType, nullable = false) ::
				StructField("puissance", DoubleType, nullable = false) ::
				StructField("pression", DoubleType, nullable = false) ::
				StructField("consommation", DoubleType, nullable= true):: Nil)

		val schmBrutStation = StructType(
			StructField("Date", StringType, nullable = false) ::
				StructField("temperature", DoubleType, nullable = false) ::
				StructField("hr", DoubleType, nullable = false) ::
				StructField("orientation", DoubleType, nullable = false) ::
				StructField("vitesse_vent", DoubleType, nullable = false) ::
				StructField("co2", DoubleType, nullable = false) ::
				StructField("capteur", DoubleType, nullable= true):: Nil)

		// connect to the database named "anjos" on port 5432 of localhost
		val url = "jdbc:postgresql://127.0.0.1:5432/anjos"
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "org.postgresql.Driver")
		connectionProperties.setProperty("user", "postgres")
		connectionProperties.setProperty("password","zYRZ2rQy42t5")

		def transformeLoadLogement(date: Date):Unit =
		{
			//println("fonction=====================================================================================================================================")
				var query = s"""(select * from brut_logement bl join capteur cp on bl.capteur = cp.id_capteur  where date >= timestamp '${date}' - interval '1 minute' and date <  timestamp '${date}' + interval '1 day' order by date,capteur desc) as brut"""
				var mesure = spark.read.jdbc(url, query, connectionProperties)



			if(mesure.count()>0) {


				//mesure.show(1);
				mesure.repartition(1)

				if (mesure.count() >= 0) {

					import spark.implicits._
					mesure = mesure.mapPartitions(iterator => {
						var statement = getConnection(url, connectionProperties)
						val res = iterator.map(row => {
							val capteur = row.getString(6)
							val reel = row.getBoolean(11)
							val piece = row.getString(9)
							var temperature =0.0
							var hr=0.0
							if(reel==true) {
								val resultSetTemp = statement.executeQuery(s"""(select fct1 from facteur_temperature where capteur_facteur='${capteur}' )""")
								resultSetTemp.first()
								val fctTemp = resultSetTemp.getDouble("fct1")
								println("========================================================== capteur date:"+capteur+" "+date)
								temperature = calculTemperature(row.getDouble(1), fctTemp)
								hr = calculHr(row.getDouble(2), temperature)
							}

							(capteur, row.getString(8),row.getString(10), row.getTimestamp(0), temperature, hr, row.getDouble(3), row.getDouble(4), row.getDouble(5), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,piece,reel)

						})
						res
					}).toDF("capteur","logement", "type", "date", "temperature", "hr", "debit_position_1", "debit_pression_1", "co2", "debit_position_2", "debit_pression_2", "debit_produit", "surface_equivalente_ea", "somme_surface_equivalente", "somme_debits_extraits", "pression_reelle", "debit_reelle", "humidite_absolue","piece","reel")


					mesure.createOrReplaceTempView("mesure_temp")

					val df_temp = spark.sql("select avg(temperature) as temperature_moyenne ,logement as logement_2,date as date_2 from mesure_temp group by logement_2,date_2")
					mesure = mesure.join(df_temp,mesure("logement")=== df_temp("logement_2") && mesure("date")===df_temp("date_2"))
					mesure = mesure.withColumn("temperature", when(col("reel")===false,col("temperature_moyenne")).otherwise(col("temperature"))).drop("temperature_moyenne").drop("date_2").drop("logement_2")

					val df_hr = spark.sql("select avg(hr) as hr_moyen ,logement as logement_2,date as date_2 from mesure_temp group by logement_2,date_2")
					mesure = mesure.join(df_hr,mesure("logement")===df_hr("logement_2") && mesure("date")===df_hr("date_2"))
					mesure = mesure.withColumn("hr", when(col("reel")===false,col("hr_moyen")).otherwise(col("hr"))).drop("hr_moyen").drop("date_2").drop("logement_2")




					mesure = mesure.mapPartitions(iterator => {
						var statement = getConnection(url, connectionProperties)
						val res = iterator.map(row => {
							//println("############################################################## partition row"+partition+" "+rowbn)
							val capteur = row.getString(0)
							val reel = row.getBoolean(19)
							val piece = row.getString(18)
							val temperature = row.getDouble(4)
							val hr = row.getDouble(5)
							//println("capteur date =========================================="+capteur+row.getTimestamp(3))
							val resultSetDp1 = statement.executeQuery(s"""(select fct1 from facteur_debit_position1 where capteur_facteur='${capteur}' ) """)
							var fctDp1 = 0.0
							while (resultSetDp1.next) {
								fctDp1 = resultSetDp1.getDouble("fct1")
							}
							val typeCapteur = getCapteurType(statement, capteur)
							var dpo1=0.0
							var dpe1=0.0
							var co2=0.0
							if(reel==true) {
								dpo1 = calculDebitPosition1(row.getDouble(6), fctDp1, piece)
								dpe1 = calculDebitPression1(row.getDouble(7))
								co2 = calculCo2(row.getDouble(8), typeCapteur)
							}
							val resultSetdp2 = statement.executeQuery(s"""(select fct1,fct2,fct3,fct4,borne_inf,borne_sup,valeur_defaut from facteur_debit_position2 where capteur_facteur='${capteur}' ) """)

							val dpo2 = calculDebitPosition2(dpo1, row.getDouble(6), hr, resultSetdp2, reel, piece)
							val resultSetDpe2 = statement.executeQuery(s"""(select fct1,fct2,fct3,borne_inf,borne_sup from facteur_debit_pression2 where capteur_facteur='${capteur}' )order by borne_inf """)
							val dpe2 = calculDebitPression2(row.getDouble(6), dpe1, resultSetDpe2, typeCapteur)
							val dp = calculDebitProduit(dpe2, dpo2, typeCapteur, piece)
							val ha = calculHumiditeAbsolue(hr, temperature)
							val seq = calculSEQ(dp, typeCapteur)
							val Séqlogement = 0.0
							val Qtotalextraitlogement = 0.0
							val pr = 0.0
							val qea = 0.0

							(capteur, row.getString(1),row.getString(2), row.getTimestamp(3), temperature, hr, dpo1, dpe1, co2, dpo2, dpe2, dp, seq, Séqlogement, Qtotalextraitlogement, pr, qea, ha)

						})
						//closeConnection(statement)
						res
					}).toDF("capteur","logement", "type", "date", "temperature", "hr", "debit_position_1", "debit_pression_1", "co2", "debit_position_2", "debit_pression_2", "debit_produit", "surface_equivalente_ea", "somme_surface_equivalente", "somme_debits_extraits", "pression_reelle", "debit_reelle", "humidite_absolue")

					//mesure = mesure.repartition(5)


					// ATTENTION CA MARCHE UN PEU

					var gb = mesure.groupBy("logement", "date").sum("debit_produit").orderBy("date")
					gb = gb.withColumnRenamed("logement", "logement_gb").withColumnRenamed("date", "date_gb")


					//mesure= mesure.repartition(5)

					mesure = mesure.join(gb, mesure("logement") === gb("logement_gb") && mesure("date") === gb("date_gb"), "inner")
					mesure = mesure.withColumn("somme_debits_extraits", when(col("type") === "Bouche", col("sum(debit_produit)")).otherwise(0.0)).drop("sum(debit_produit)")

					//mesure = mesure.repartition(5)
					//println("========================================================================== fin  calcul somme debits extraits")

					gb = mesure.groupBy("logement", "date").sum("surface_equivalente_ea").orderBy("date")
					gb = gb.withColumnRenamed("logement", "logement_gb").withColumnRenamed("date", "date_gb")

					mesure = mesure.join(gb, mesure("logement") === gb("logement_gb") && mesure("date") === gb("date_gb"), "inner")
					mesure = mesure.withColumn("somme_surface_equivalente", when(col("type") === "EA", col("sum(surface_equivalente_ea)")).otherwise(0.0)).drop("sum(surface_equivalente_ea)")

					//println("========================================================================== fin  calcul somme surface equivalente")

					//mesure.show(10)

					import spark.implicits._
					mesure = mesure.mapPartitions(iterator => {
						var statement = getConnection(url, connectionProperties)

						val res = iterator.map(row => {
							var SeqLogement = 0.0
							val logement = row.getString(1)
							if (row.getString(2) == "EA") {

								val rs = statement.executeQuery(s"""(select fct1 from facteur_somme_surface_equivalente where logement_facteur='${logement}' )""")
								rs.first()
								val fct1 = rs.getDouble("fct1")
								SeqLogement = fct1 + row.getDouble(11)
							}

							(row.getString(0), logement, row.getTimestamp(3), row.getDouble(4), row.getDouble(5), row.getDouble(6), row.getDouble(7), row.getDouble(8), row.getDouble(9), row.getDouble(10), row.getDouble(11), row.getDouble(12), SeqLogement, row.getDouble(14), row.getDouble(15), row.getDouble(16), row.getDouble(17))
						})
						//closeConnection(statement)
						res
					}).toDF("capteur", "logement", "date", "temperature", "hr", "debit_position_1", "debit_pression_1", "co2", "debit_position_2", "debit_pression_2", "debit_produit", "surface_equivalente_ea", "somme_surface_equivalente", "somme_debits_extraits", "pression_reelle", "debit_reelle", "humidite_absolue")


					var QtotalExtraitLogement = mesure.groupBy("logement", "date").max("somme_debits_extraits")
					QtotalExtraitLogement = QtotalExtraitLogement.withColumn("logement_gb", col("logement")).withColumn("date_gb", col("date")).drop("logement").drop("date")

					var SeqLogement = mesure.groupBy("logement", "date").max("somme_surface_equivalente")
					SeqLogement = SeqLogement.withColumn("logement_gb", col("logement")).withColumn("date_gb", col("date")).drop("logement").drop("date")


					mesure = mesure.join(QtotalExtraitLogement, mesure("logement") === QtotalExtraitLogement("logement_gb") && mesure("date") === QtotalExtraitLogement("date_gb"), "inner").drop("date_gb").drop("logement_gb")
					mesure = mesure.join(SeqLogement, mesure("logement") === SeqLogement("logement_gb") && mesure("date") === SeqLogement("date_gb"), "inner").drop("date_gb").drop("logement_gb")


					val debit_reelle = udf[Double, Double, Double](calculDebitReelle)

					mesure = mesure.drop("somme_debits_extraits").drop("somme_surface_equivalente").withColumnRenamed("max(somme_debits_extraits)", "somme_debits_extraits").withColumnRenamed("max(somme_surface_equivalente)", "somme_surface_equivalente").withColumn("pression_reelle", col("somme_debits_extraits") / (col("somme_surface_equivalente") * col("somme_surface_equivalente"))).drop("logement")
					mesure = mesure.withColumn("debit_reelle", debit_reelle(col("somme_surface_equivalente"), col("pression_reelle")))


					val formatString = new java.text.SimpleDateFormat("yyyy-MM-dd")
					var dateString = formatString.format(date)


					mesure = mesure.filter(mesure("date") >= lit(dateString))


					try {
					val statement = getConnection(url, connectionProperties)
					statement.executeQuery(s"""delete from test_logement where date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
				} catch {
					case  e : Exception =>{}
				}

					mesure.write
						.mode(SaveMode.Append).jdbc(url, "test_logement", connectionProperties)
				}
			}


		}

		def transformeLoadStation(date :Date): Unit =
		{

			var query = s"""(select * from brut_station bs where date >= timestamp '${date}' - interval '1 minute' and date <  timestamp '${date}' + interval '1 day') as brut"""
			var mesure = spark.read.jdbc(url, query, connectionProperties)

			if(mesure.count()>0) {
				var mesurecolsA = mesure.columns
				var mesurecols = collection.mutable.ArrayBuffer(mesurecolsA: _*)

				mesurecols -= "date"
				mesurecols -= "id"


				for (colname <- mesurecols) {
					mesure = supprErreur(mesure, colname)
				}

				import spark.implicits._
				mesure = mesure.mapPartitions(iterator => {
					var statement = getConnection(url, connectionProperties)

					val res = iterator.map(row => {
						val capteur = row.getString(6)
						val resultSetTemp = statement.executeQuery(s"""(select fct1 from facteur_temperature where capteur_facteur='${capteur}' ) """)
						resultSetTemp.first()
						val fctTemp = resultSetTemp.getDouble("fct1")
						val temperature = calculTemperature(row.getDouble(1), fctTemp)
						val hr = calculHr(row.getDouble(2), temperature)
						val orientation = calculOrientation(row.getDouble(3))
						val vitesseVent = calculVitesseVent(row.getDouble(4))
						val co2 = calculCo2(row.getDouble(5),"Station")
						val bousole = orientationBousole(orientation)

						(row.getTimestamp(0), temperature, hr, orientation, vitesseVent, co2, bousole,capteur)
					})
					//closeConnection(statement)
					res
				}).toDF("date","temperature","hr","orientation","vitesse_vent","co2","bousole","capteur")

				val timestamp = new java.sql.Timestamp(date.getTime)


				mesure = mesure.filter(mesure("date") >= timestamp)



				try {
			val statement = getConnection(url, connectionProperties)
			statement.executeQuery(s"""delete from test_station where date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
			} catch {
			case  e : Exception =>{}
			}

			mesure.write
				.mode(SaveMode.Append).jdbc(url, "test_station", connectionProperties)
		}
		}

		def transformeLoadCaisson(date: Date): Unit =
		{
			var query = s"""(select * from brut_caisson bs where date >= timestamp '${date}' - interval '1 minute' and date <  timestamp '${date}' + interval '1 day') as brut"""
			var mesure = spark.read.jdbc(url, query, connectionProperties)

			if(mesure.count()>0) {
				var mesurecolsA = mesure.columns
				var mesurecols = collection.mutable.ArrayBuffer(mesurecolsA: _*)

				mesurecols -= "date"
				mesurecols -= "id"

				for (colname <- mesurecols) {
					mesure = supprErreur(mesure, colname)
				}

				import spark.implicits._
				mesure = mesure.mapPartitions(iterator => {
					var statement = getConnection(url, connectionProperties)

					val res = iterator.map(row => {
						val capteur = row.getString(4)
						val puissance = calculPuissance(row.getDouble(1))
						val pression = calculPression(row.getDouble(2))
						val consomation = 0.0

						(row.getTimestamp(5), puissance, pression, consomation,capteur)
					})
					//closeConnection(statement)
					res
				}).toDF("date","puissance","pression","consommation","capteur")

				val timestamp = new java.sql.Timestamp(date.getTime)


				mesure = mesure.filter(mesure("date") >= timestamp)



				try {
					val statement = getConnection(url, connectionProperties)
					statement.executeQuery(s"""delete from test_caisson where date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
				} catch {
					case  e : Exception =>{}
				}

				mesure.write
					.mode(SaveMode.Append).jdbc(url, "test_caisson", connectionProperties)
			}
		}


		def extractLogement(capteurFile: File): DataFrame =
		{
			val capteurFilePath = capteurFile.getPath
			val capteurFileName = capteurFile.getName
			val capteurBoitier = getCapteurBoitier(capteurFileName)


			var brut = spark.read.option("delimiter", "\t").schema(schmBrut).csv(capteurFilePath).toDF
			brut = brut.withColumn("date", to_timestamp(col("Date"),"dd/MM/yyyy HH:mm"))
			brut = brut.withColumn("capteur", lit(capteurBoitier) )

			var mesurecolsA = brut.columns
			var mesurecols = collection.mutable.ArrayBuffer(mesurecolsA: _*)

			mesurecols -= "date"
			mesurecols -= "id"

			for (colname <- mesurecols) {
				brut = supprErreur(brut, colname)
			}

			/*if (brut.count() > 0) {
				val date = brut.select("date").orderBy("date").first().getTimestamp(0)


				try {
					val statement = getConnection(url, connectionProperties)
					statement.executeQuery(s"""delete from brut_logement where capteur='${capteurBoitier}' and date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
				} catch {
					case e: Exception => {}
				}
			}*/
			brut.write
				.mode(SaveMode.Append).jdbc(url, "brut_logement", connectionProperties)
			return brut
		}

		def extractCaisson(capteurFile: File): DataFrame = {
			val capteurFilePath = capteurFile.getPath
			val capteurFileName = capteurFile.getName
			val capteurBoitier = getCapteurBoitier(capteurFileName)


			var brut = spark.read.option("delimiter", "\t").schema(schmBrutCaisson).csv(capteurFilePath).toDF
			brut = brut.withColumn("date", to_timestamp(col("Date"), "dd/MM/yyyy HH:mm")).drop("vide1").drop("vide2").drop("vide3")
			brut = brut.withColumn("capteur", lit(capteurBoitier))


			if (brut.count() > 0.0){
			val date = brut.select("date").orderBy("date").first().getTimestamp(0)

			try {
				val statement = getConnection(url, connectionProperties)
				statement.executeQuery(s"""delete from brut_caisson where capteur='${capteurBoitier}' and date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
			} catch {
				case e: Exception => {}
			}}

			brut.write
				.mode(SaveMode.Append).jdbc(url, "brut_caisson", connectionProperties)
			return brut

		}

		def extractStation(capteurFile: File): DataFrame =
		{
			import spark.implicits._
			val capteurFilePath = capteurFile.getPath
			val capteurFileName = capteurFile.getName
			val capteurBoitier = getCapteurBoitier(capteurFileName)


			var brut = spark.read.option("delimiter", "\t").schema(schmBrutStation).csv(capteurFilePath).toDF
			brut = brut.withColumn("date", to_timestamp(col("Date"),"dd/MM/yyyy HH:mm")).drop("vide1").drop("vide2").drop("vide3")
			brut = brut.withColumn("capteur", lit(capteurBoitier) )

			if (brut.count() > 0.0) {
				val date = brut.select("date").orderBy("date").first().getTimestamp(0)


				try {
					val statement = getConnection(url, connectionProperties)
					statement.executeQuery(s"""delete from brut_station where  date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day'""")
				} catch {
					case e: Exception => {}
				}
			}


			brut.write
				.mode(SaveMode.Append).jdbc(url, "brut_station", connectionProperties)
			return brut
		}

		def createGostMesure(dateDebut : Date,dateFin : Date, capteur: String): DataFrame =
		{

			var date = dateDebut
			var timestamp = new Timestamp(date.getTime)
			var seq = Seq((timestamp, 0, 0, 0, 0, 0, capteur))
			val calAddoneMin = Calendar.getInstance


			calAddoneMin.setTime(date)
			calAddoneMin.add(Calendar.MINUTE,1)
			date = calAddoneMin.getTime()
			while (date!=dateFin) {
				timestamp = new Timestamp(date.getTime)
				seq = seq ++ Seq((timestamp, 0, 0, 0, 0, 0, capteur))
				calAddoneMin.setTime(date)
				calAddoneMin.add(Calendar.MINUTE,1)
				date = calAddoneMin.getTime()
				//println(date)
			}
			var df = seq.toDF("date","temperature","hr","debit_position-1","debit_pression_1","co2","capteur")
			df = df.withColumn("date", to_timestamp(col("date")))
			//df.show(1440)
			df
		}

		def extractGostValue(date: Date): Unit =
		{

			val calAddOneDay = Calendar.getInstance
			calAddOneDay.setTime(date)
			calAddOneDay.add(Calendar.DATE,1)
			val dateMax = calAddOneDay.getTime()


			var brut = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schmBrut)
			brut = brut.withColumn("date", to_timestamp(col("date")))
			val statement = getConnection(url, connectionProperties)
			val rs = statement.executeQuery("select id_capteur from capteur where reel=false")

			//println()
			while (rs.next())
			{
				brut = brut.union(createGostMesure(date,dateMax,rs.getString("id_capteur")))
			}


			brut.write
				.mode(SaveMode.Append).jdbc(url, "brut_logement", connectionProperties)

		}

		var filelist = recursiveListFiles(new File("/home/n2m/Documents/TEST"))
		//var filelist = recursiveListFiles(new File("/home/n2m/Documents/Test_erreur"))


		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
		val formatString = new java.text.SimpleDateFormat("dd MM yyyy")
		val dateMin = format.parse(args(0))
		val dateMax = format.parse(args(1))

		var date = dateMin
		var dateString = formatString.format(date)
		val c = Calendar.getInstance
		while (date != dateMax) {

			val fileCapteur= filelist.filter(f => (""".+(162|093|088|182|177|223)+ le """+dateString+"""+\.acq$""").r.findFirstIn(f.getName).isDefined)
			val fileCaisson= filelist.filter(f => ("""caisson+.+le """+dateString+"""+\.acq$""").r.findFirstIn(f.getName).isDefined)
			val fileStation= filelist.filter(f => ("""Station météo+.+le """+dateString+"""+\.acq$""").r.findFirstIn(f.getName).isDefined)

			try {
				val statement = getConnection(url, connectionProperties)
				statement.executeQuery(s"""delete from brut_logement where date >= timestamp '${date}' and date <= timestamp '${date}' + interval '1 day' - interval '1 minute'""")
			} catch {
				case e: Exception => {}
			}

			for (file <- fileCapteur)
			{
				extractLogement(file)
			}

			extractGostValue(date)

			if(fileCaisson.length>0) {
				extractCaisson(fileCaisson(0))
			}

			if(fileStation.length>0) {
				extractStation(fileStation(0))
			}

			transformeLoadLogement(date)
			transformeLoadStation(date)
			transformeLoadCaisson(date)

			c.setTime(date)
			c.add(Calendar.DATE,1)
			date = c.getTime()

			dateString = formatString.format(date)
			//println("date =============================="+dateString)
		}




		spark.stop()
	}
}

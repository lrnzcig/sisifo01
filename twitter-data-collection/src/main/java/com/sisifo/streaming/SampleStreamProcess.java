/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.sisifo.streaming;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sisifo.twitter_model.tweet.Tweet;
import com.sisifo.twitter_model.tweet.TweetInstanceCreator;
import com.sisifo.twitter_model.utils.FavoriteFileWriter;
import com.sisifo.twitter_model.utils.FriendsFileWriter;
import com.sisifo.twitter_model.utils.TweetFileWriter;
import com.sisifo.twitter_rest_client.utils.CommonTwitterUtils;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class SampleStreamProcess {
	
	private static final int MAX_NUMBER_OF_MESSAGES = 10000000;

	public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
		// code from hbc-example SampleStreamExample !!
		
		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.stallWarnings(false);
		
		endpoint.languages(Lists.newArrayList(CommonTwitterUtils.LANGUAGE));
		// TODO input file
		/*
		endpoint.trackTerms(Lists.newArrayList("@ahorapodemos,@pablo_iglesias,@ciudadanoscs,@albert_rivera,@ppopular,@marianorajoy,"
			+ "@psoe,@sanchezcastejon,@upyd,@rosadiezupyd,@_anapastor_,@iescolar,@pedroj_ramirez,@abarceloh25,@ccarnicero,"
			+ "@melchormiralles,@garcia_abadillo,@pacomarhuenda,@fgarea,@montsehuffpost,@carloscuestaem,@alfonsomerlos,"
			+ "@oneto_p,@antonio_cano,@bieitorubido,@psebastianbueno"));
			*/
		endpoint.trackTerms(Lists.newArrayList("@Cristiano,@realmadrid,@andresiniesta8,@3gerardpique,@XabiAlonso,@enrique305,@aguerosergiokun,"
			+ "@FCBarcelona_es,@Carles5puyol,@cesc4official,@RafaelNadal,@davidbisbal,@jamesdrodriguez,@Guaje7Villa,@GarethBale11,@SergioRamos,"
			+ "@muyinteresante,@realmadriden,@FCBarcelona_cat,@juanmata8,@D_DeGea,@CasillasWorld,@el_pais,@MarceloM12,@1victorvaldes,@aarbeloa17,"
			+ "@belindapop,@_Pedro17_,@21LVA,@marca,@paugasol,@mtvspain,@isco_alarcon,@Torres,@pabloalboran,@Thiago6,@SSantiagosegura,@ToniKroos,"
			+ "@AlvaroMorata,@Buenafuente,@ifilosofia,@alo_oficial,@ristomejide,@LigaBBVA,@danimartinezweb,@Fabio_Coentrao,@raphaelvarane,@DeboSuponerQue,"
			+ "@officialpepe,@CristiPedroche,@llorentefer19,@BoseOfficial,@ctello91,@AnnaSimonMari,@El_Hormiguero,@MrAncelotti,@elmundoes,@rickyrubio9,"
			+ "@Berto_Romero,@mario_casas_,@MarcBartra,@jordievole,@19SCazorla,@JeseRodriguez10,@DANIROVIRA,@mundodeportivo,@policia,@MelendiOficial,@Atleti,"
			+ "@flofdezz,@GUTY14HAZ,@_MaluOficial_,@diarioas,@marcmarquez93,@_danielmartin_,@Cosmopolitan_es,@DaniCarvajal92,@2010MisterChip,"
			+ "@JordiAlba,@pacoleonbarrios,@Javi8martinez,@SergiRoberto10,@riverakiko,@hola,@manuchao,@SeFutbol,@lorenzo99,@BelenEstebanM,@perezreverte,"
			+ "@Rafinha,@PReina25,@rudy5fernandez,@nachofi1990,@VogueSpain,@AnderHerrera,@R_Albiol,@DaniMateoAgain,@hdnegro,@albertocontador,@Los40_Spain,"
			+ "@_Ciencia_Tecno,@13_Pinto,@eGranero11,@Carlos_Latre,@androides,@David_Busta,@lafabricacrm,@A3Noticias,@estopaoficial,"
			+ "@GuillemBalague,@GLaraLopez,@blogdecine,@antena3com,@sport,@R9Soldado,@DavidFerrer87,@rtve,@20m,@MarcGasol,@RecetasdeCocina,@JoseMotatv,"
			+ "@adidas_ES,@gerardeulofeu,@RAEinforma,@RecetasLigeras,@RAFAMORATETE,@2MONTOYA,,@NavasKeylor,@IkerMuniain19,@RealPorta,@eva_hache,"
			+ "@SergioCanales,@abc_es,@EFEnoticias,@PilarRubio_,@maldinisport,@epunset,@telecincoes,@CuencaIsaac,@Juanfrantorres,@Sabinaquotes,@diegomcea,"
			+ "@elchiringuitotv,@CesarAzpi,@applesfera,@AbrahamMateoMus,@AlvaroNegredo_7,@javigarcia06,@JulianAveiro_,@mujerdehoy,@As_TomasRoncero,@elle_es,"
			+ "@PiedrahitaLuis,@Koke6,@La_SER,@Kiko_Hernandez,@elpais_deportes,@AlbaRicoNavarro,@fertejerom,@Borisizaguirre,@publico_es,@El_Intermedio,"
			+ "@HolaBelleza,@Edurnity,@tublogdecine,@RevillaMiguelA,@Nosotras_com,@GustavoSBR,@rfef,@europapress,@JuanMagan,@monicanaranjo,"
			+ "@MarioSuarez4,@greenpeace_esp,@LUISENRIQUE21,@_JesusVazquez_,@laSextaTV,@valenciacf,@AngyFdz,@adrian7oficial,@microsiervos,@antoniorozco,"
			+ "@laorejadevgogh,@muniesa92,@salvadostv,@NadalMiki,@NataliaJimenez,@Llourinho,@allpeim,@julia_otero,@jpedrerol,@Lecturalia,@mariavalverde,"
			+ "@lavoztelecinco,@24h_tve,@vigalondo,@NoelBayarri,@raqsanchezsilva,@Declaracion,@AdrianoCorreia6,@carlosjean,@EnElAireAB,@AmaiaMontero,"
			+ "@VanityFairSpain,@cocina_es,@ursulolita,@SevillaFC,@manucarreno,@elmundotoday,@albertochicote,@myh_tv,@FCBbasket,@Tamara_Gorro,@phernandez19,"
			+ "@alexdelaIglesia,@abeniaadriana,@cuatro,@CarmendMairena,@LaVanguardia,@Goyojimenez,@la_informacion,@dbravo,@marieclaire_es,@Michuoviedo,"
			+ "@maria_hb18,@TuCaraMSuena,@LaSextaAsiNosVa,@cookpadrecetas,@michaelrobinson,@Santi_Millan,@AS_Futbol,@elEconomistaes,@susannagriso,"
			+ "@elpais_cultura,@AthleticClub,@lamacope,@davidguapo,@cuore,@gizmodoES,@JmCalderon3,@PipiEstrada1,@eljueves,@museodelprado"
			// hasta aquí, top 250 menos @_anapastor_, @iescolar, @ahorapodemos, @marianorajoy, y @Pablo_Iglesias_
			+ "@ahorapodemos,@pablo_iglesias,@ciudadanoscs,@albert_rivera,@ppopular,@marianorajoy,"
			+ "@psoe,@sanchezcastejon,@upyd,@rosadiezupyd,@_anapastor_,@iescolar,@pedroj_ramirez,@abarceloh25,@ccarnicero,"
			+ "@melchormiralles,@garcia_abadillo,@pacomarhuenda,@fgarea,@montsehuffpost,@carloscuestaem,@alfonsomerlos,"
			+ "@oneto_p,@antonio_cano,@bieitorubido,@psebastianbueno"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
            .name("sampleExampleClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

		// Establish a connection
		client.connect();

		TweetFileWriter w = new TweetFileWriter("streaming");
		FriendsFileWriter fw = new FriendsFileWriter("streaming");
		FavoriteFileWriter favw = new FavoriteFileWriter("streaming");
		UserInfoThread thread = new UserInfoThread();
		thread.startup(fw, favw, consumerKey, consumerSecret);
		thread.start();

		for (int msgRead = 0; msgRead < MAX_NUMBER_OF_MESSAGES; msgRead++) {
			if (client.isDone()) {
				System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
				break;
			}
			if (msgRead % 1000 == 0) {
				System.out.println(msgRead + " messages retrieved.");
				w.flush();
			}

			String msg = queue.poll(5, TimeUnit.SECONDS);
			if (msg == null) {
				System.out.println("Did not receive a message in 5 seconds");
			} else {
				// convert - GSON
				GsonBuilder builder = new GsonBuilder();
				builder.registerTypeAdapter(Tweet.class, new TweetInstanceCreator());
				Gson gson = builder.create();
				Tweet tweet = gson.fromJson(msg, Tweet.class);
				// write CSV
				w.writeToFile(tweet);
				getUserAdditionalInfo(tweet, thread);
			}
		}

		w.close();
		client.stop();

		// Print some stats
		System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}

	private static void getUserAdditionalInfo(Tweet tweet, UserInfoThread thread) {
		if (tweet.getUser() == null) {
			// TODO ?????
			// reject the tweet ?
			return;
		}
		// always add user; the thread only fetches it if it has not been added before
		Long userId = tweet.getUser_id();
		thread.addUserId(userId);
	}

	public static void main(String[] args) {
		try {
			SampleStreamProcess.run(args[0], args[1], args[2], args[3]);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}

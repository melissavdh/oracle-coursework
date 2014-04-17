import java.io.*;
import java.sql.*;
import java.lang.*;
import java.util.regex.*;


class CWK3 {

   static Connection conn = null;
   Statement stat_ex;
   Statement stat_exQ;
   Statement stat_exU;
   ResultSet resultset;
   int resultint = 0;

   public enum Input {
      login, who, schema, transponder, race, riderdata, circuit, unit, print, postrace, addpoints, penalty, printadjusted, setpoints;
   }

   public boolean execute(String query, int type) {
      try {
         if (type == 0) {
            if (stat_ex != null) stat_ex.close();
            stat_ex = conn.createStatement();
            stat_ex.execute(query);
         }
         else if (type == 1) {
            if (stat_exQ != null) stat_exQ.close();
            stat_exQ = conn.createStatement();
            resultset = stat_exQ.executeQuery(query);
         }
         else if (type == 2) {
            if (stat_exU != null) stat_exU.close();
            stat_exU = conn.createStatement();
            resultint = stat_exU.executeUpdate(query);
         }
         return true;
      }
      catch (SQLException e) {
         //e.printStackTrace();
         return false;
      }
   }


   void create_table() throws SQLException {
      String[] query = {
         "login (username CHAR(6))",
         "transponder (transponder INT, crosstime INT, lapno INT, laptime INT)",
         "riderdata (ridernumber INT, ridername VARCHAR2(50), ridersurname VARCHAR2(50),riderteam VARCHAR2(50), points INT)",
         "circuit (circuit VARCHAR2(30), winner INT, winmargin INT, fastestlaprider INT, fastestlaptime INT, numberoflaps INT, finishers INT)",
         "current_circuit (current_circuit VARCHAR2(30), starttime NUMBER, stopped INT, addpoints INT)",
         "unit (ridernumber INT, transponder INT)",
         "points (ridernumber INT, circuit VARCHAR2(30), points INT)",
         "penalty (ridernumber INT, team VARCHAR2(30), new_points INT)"
      };
      for (int i=0; i<query.length; i++)
         execute("CREATE TABLE " + query[i], 0);
   }

   void drop_table(String table_name) throws SQLException {
      execute("DROP TABLE " + table_name, 0);
   }

   void login(String user_name) throws SQLException{
      try {
         Pattern pattern = Pattern.compile("[a-z]{2}\\d{4}");
         Matcher matcher = pattern.matcher(user_name);
         if (matcher.matches()) {
            execute("TRUNCATE TABLE login", 0);
            if (execute("INSERT INTO login (username) VALUES ('" + user_name + "')", 0)) {
               System.out.println("success");
            } else {
               System.out.println("failure");
            }
         }
      }
      catch (Exception e) {
         System.out.println("failure");
      }
   }

   void who() throws SQLException {
      execute("SELECT username FROM login", 1);
      while (resultset.next())
         System.out.println(resultset.getString(1));
   }

   void schema(String action) throws SQLException {
      if (action.equals("create")) {
         execute("SELECT * FROM user_tables", 2);
         if (resultint == 0) {
            create_table();
            System.out.println("success");
         }
      }
      else if (action.equals("drop")) {
         execute("SELECT * FROM user_tables", 1);
         execute("SELECT * FROM user_tables", 2);
         if (resultint != 0) {
            while (resultset.next()) {
               drop_table(resultset.getString(1)); }
            System.out.println("success");
         }
      }
   }


   void transponder(String ridertransponder) throws SQLException {

      // check transponder number exists
      execute("SELECT * FROM unit WHERE transponder = " + ridertransponder, 2);
      if (resultint != 0) {

         // check race hasn't stopped
         execute("SELECT stopped FROM current_circuit WHERE stopped = 1", 2);
         if (resultint != 1) {

            // retrieve starttime from current_circuit to calculate time passed
            execute("SELECT starttime FROM current_circuit", 1);
            long start_time = 0;
            while (resultset.next())
               start_time = resultset.getLong(1);

            // get cross time
            long cross_time = System.currentTimeMillis();

            // find total number of entries of rider in transponder table
            execute("SELECT * FROM transponder WHERE transponder = " + ridertransponder, 2);
            int num_entries = resultint;

            // calculate lap number rider just started
            int lapno = 0;
            if (num_entries == 0) lapno = 1;
            else lapno = num_entries + 1;

            // insert all the data into transponder table
            execute("INSERT INTO transponder (transponder, crosstime, lapno) VALUES (" + ridertransponder + ", " + cross_time + ", " + lapno + ")", 0);

            // calculate time elapsed
            long elapsed_time = cross_time - start_time;
            System.out.println(elapsed_time/1000);

            // calculate lap time
            execute("SELECT MAX(lapno) FROM (SELECT lapno FROM transponder WHERE transponder = " + ridertransponder + " ORDER BY lapno DESC)", 1);
            resultset.next();
            int lapnum = resultset.getInt(1);
            if (lapnum > 1) {
               execute("SELECT crosstime FROM transponder WHERE transponder = " + ridertransponder + " AND lapno = " + (lapnum-1), 1);
               resultset.next();
               long prev_time = resultset.getLong(1);
               long lap_time = cross_time - prev_time;
               execute("UPDATE transponder SET laptime = " + lap_time + " WHERE transponder = " + ridertransponder + " AND lapno = " + (lapnum-1), 0);
            }
         }
      }
   }

   void race(String command) throws SQLException {
      if (command.equals("start")) {

         // check current circuit has been chosen
         execute("SELECT * FROM current_circuit", 2);
         int exists = resultint;
         if (exists == 1) {

//            // empty transponder table for new race
//            execute("TRUNCATE TABLE transponder", 0);

            // start timing
            long start_time = System.currentTimeMillis();

            // add start time to current_circuit
            execute("UPDATE current_circuit SET starttime = " + start_time, 0);

            System.out.println("success");
         }
      }
      else if (command.equals("stop")) {

         // check race has started
         execute("SELECT starttime from current_circuit WHERE starttime IS NULL", 2);
         int started = resultint;
         if (started == 0) {

            // check race hasn't already been stopped
            execute("SELECT stopped FROM current_circuit WHERE stopped = 1", 2);
            int has_stopped = resultint;

            if (has_stopped == 0) {

               // set race as stopped
               execute("UPDATE current_circuit SET stopped = 1", 0);

               // get transponder, last crosstime, no laps completed by winner
               execute("SELECT transponder, crosstime, lapno FROM (SELECT transponder, MAX(crosstime) AS crosstime, MAX(lapno) AS lapno FROM transponder GROUP BY transponder ORDER BY lapno DESC, crosstime ASC) where rownum <=2", 1);
               long winner = 0;
               long winner_time = 0;
               int total_laps = 0;
               if(resultset.next()) {
                  winner = resultset.getLong(1);
                  winner_time = resultset.getLong(2);
                  total_laps = (resultset.getInt(3))-1;
               }

               // get last crosstime of second place
               long second_time = 0;
               if (resultset.next()) second_time = resultset.getLong(2);

               // calculate win margin
               long winmargin = (second_time - winner_time)/1000;

               // get transponder number, time of fastest lap
               execute("SELECT * FROM (SELECT transponder, laptime FROM transponder WHERE laptime IS NOT NULL ORDER BY laptime ASC) WHERE rownum = 1", 1);
               int flap_rider = 0;
               long flap_time = 0;
               if (resultset.next()) {
                  flap_rider = resultset.getInt(1);
                  flap_time = (resultset.getLong(2))/1000;
               }
   
               // get number of riders who completed same number of laps as winner
               execute("SELECT COUNT(transponder) FROM transponder WHERE lapno = " + (total_laps+1), 1);
               int finishers = 0;
               if (resultset.next()) finishers = resultset.getInt(1);

               // add data to circuit table
               execute("INSERT INTO circuit (circuit, winner, winmargin, fastestlaprider, fastestlaptime, numberoflaps, finishers) SELECT current_circuit.current_circuit, " + winner +".dual, " + winmargin + ".dual, " + flap_rider + ".dual, " + flap_time + ".dual, " + total_laps + ".dual, " + finishers + ".dual FROM current_circuit, dual", 0);

               System.out.println("success");
            }
         }
      }
   }

   void riderdata(String all_data) throws SQLException {
      // split input into relevant parts
      String[] data = all_data.split(":");
      int rider_number = Integer.parseInt(data[0]);
      String rider_name = data[1];
      String rider_surname = data[2];
      String rider_team = data[3];

      // check rider number between 1 and 999
      if ( !(rider_number < 1) && !(rider_number > 999) ) {

         // search table, if number or name exists then failure
         execute("SELECT * FROM riderdata WHERE ridernumber = " + rider_number, 2);
         int exists_num = resultint;
         execute("SELECT * FROM riderdata WHERE LOWER(ridername) = LOWER('" + rider_name + "') AND LOWER(ridersurname) = LOWER('" + rider_surname + "')", 2);
         int exists_name = resultint;

         if ( (exists_num != 0) || (exists_name != 0) ) System.out.println("failure, already exists");
         else {
            // insert the data into the riderdata table
            execute("INSERT INTO riderdata (ridernumber, ridername, ridersurname, riderteam, points) VALUES ('" + rider_number + "', '" + rider_name + "', '" + rider_surname + "', '" + rider_team + "', 0) ", 0);
            System.out.println("success");
         }
      }
   }

   void circuit(String circuit) throws SQLException {
      //search circuit, if it already exists throw exception
      execute("SELECT * FROM circuit WHERE LOWER(circuit) = LOWER('" + circuit + "')", 2);
      int exists = resultint;
      if (exists == 0) {

         // if race started cant change circuit
         execute("SELECT * from current_circuit WHERE starttime IS NOT NULL AND stopped IS NULL", 2);
         int started = resultint;
         if (started != 1) {

            // empty current circuit table
            execute("TRUNCATE TABLE current_circuit", 0);

            // empty transponder table for new race /////////////////////////
            execute("TRUNCATE TABLE transponder", 0); ///////////////////////

            //set current_circuit to circuit name
            execute("INSERT INTO current_circuit (current_circuit) VALUES ('" + circuit + "')", 0);

            // empty unit table ready for new transponder numbers for the circuit
            execute("TRUNCATE TABLE unit", 0);

            System.out.println("success");
         }
      }
      else System.out.println("failure");
   }

   void unit(String input) throws SQLException {
      // if race started, dont add to unit table
      execute("SELECT * FROM current_circuit WHERE starttime IS NULL", 2);
      int started = resultint;
      if (started == 1) {

         // split input into relevant parts
         String[] data = input.split(":");
         int ridernumber = Integer.parseInt(data[0]);
         int transponder = Integer.parseInt(data[1]);

         // check ridernumber has been registered
         execute("SELECT * FROM riderdata WHERE ridernumber = " + ridernumber, 2);
         int exists_num = resultint;
         if (exists_num != 0) {

            // check transponder between 1 and 9999
            if ( (transponder >= 1) && (transponder <= 9999) ) {

               // check ridernumber doesn't already exist in unit table
               execute("SELECT * FROM unit WHERE ridernumber = " + ridernumber, 2);
               int exists_rider = resultint;
               if (exists_rider == 0) {

                  // check transponder doesn't already exist in unit table
                  execute("SELECT * FROM unit WHERE transponder = " + transponder, 2);
                  int exists_trans = resultint;
                  if (exists_trans == 0) {

                     // insert rider number and transponder into unit table
                     execute("INSERT INTO unit (ridernumber, transponder) VALUES ('" + ridernumber + "', '" + transponder + "') ", 0);

                     // print out rider's full name and transponder
                     execute("SELECT riderdata.ridername, riderdata.ridersurname, unit.transponder FROM riderdata, unit WHERE unit.ridernumber=riderdata.ridernumber AND riderdata.ridernumber = " + ridernumber, 1);
                     while (resultset.next()) System.out.println(resultset.getString(1) + " " + resultset.getString(2) + "\t" + resultset.getString(3));
                  }
               }
            }
         }
      }
   }

   void print(String command) throws SQLException {
      // only print leaderboard if "print leaderboard" called
      if (command.equals("leaderboard")) {

         // find current lap = highest lap number in transponder table
         execute("SELECT MAX(lapno) FROM transponder", 1);
         resultset.next();
         int lap = resultset.getInt(1);

         // find the 10 leading riders in descending order
         execute("SELECT transponder, MAX(crosstime) AS crosstime, MAX(lapno) AS lapno FROM transponder GROUP BY transponder ORDER BY lapno DESC, crosstime ASC", 1);
         System.out.println("LAP	" + lap);
			int counter = 0;
         while (resultset.next())
         {
				counter++;
            if (counter > 10) break;
            System.out.println(counter + "\t" + resultset.getInt(1));
         }
      }
      else if (command.equals("standings")) {
         try {
            execute("SELECT ridernumber, points FROM riderdata ORDER BY points DESC", 1);
            while (resultset.next())
            {
               System.out.println(resultset.getInt(1) + "\t" + resultset.getInt(2));
            }
            conn.commit();
         }
         catch (SQLException e) {
//            conn.rollback();
         }
      }
   }

   void postrace(String circuit) throws SQLException {
      execute("SELECT winner, winmargin, fastestlaprider, fastestlaptime, numberoflaps, finishers FROM circuit WHERE LOWER(circuit) = LOWER('" + circuit + "')", 1);
      resultset.next();
      int winner = resultset.getInt(1);
      System.out.println("winner\t" + winner);
      System.out.println("winmargin\t" + resultset.getInt(2));
      System.out.println("fastestlaprider\t" + resultset.getInt(3));
      System.out.println("fastestlaptime\t" + resultset.getInt(4));
      System.out.println("numberoflaps\t" + resultset.getInt(5));
      System.out.println("finishers\t" + resultset.getInt(6));

      conn.setAutoCommit(false);
      try {

         execute("SELECT riderdata.points FROM riderdata, unit WHERE unit.ridernumber=riderdata.ridernumber AND unit.transponder = " + winner, 1);
         int winner_points = 0;
         while (resultset.next()) winner_points = resultset.getInt(1);
         System.out.println("winner points = " + winner_points);

         execute("SELECT * FROM circuit", 2);
         System.out.println("num saved circuits = " + resultint);
         float average = winner_points/resultint;
         conn.commit();
         System.out.println("winneraverage\t" + average);
      }
      catch (SQLException e) {
      }
   }

   void addpoints(String raceresult) throws SQLException {
      try {
         // only continue if "addpoints raceresult" input
         if (raceresult.equals("raceresult")) {

            // only continue if points not already added for current circuit
            execute("SELECT * FROM current_circuit WHERE current_circuit IS NOT NULL AND stopped = 1 AND addpoints IS NULL", 2);
            if (resultint == 1) {

               int[] points = {25,20,16,13,11,10,9,8,7,6,5,4,3,2,1};

               // find the 15 leading riders in descending order
               execute("SELECT transponder, MAX(crosstime) AS crosstime, MAX(lapno) AS lapno FROM transponder GROUP BY transponder ORDER BY lapno DESC, crosstime ASC", 1);
	      		int i = 0;
               while (resultset.next())
               {
         			i++;
                  if (i > 15) break;
                  execute("UPDATE riderdata SET points = points + " + points[i-1] + " WHERE ridernumber = (SELECT ridernumber FROM unit WHERE transponder = " + resultset.getInt(1) + ")", 0);
               }
               conn.commit();
               System.out.println("success");
            }
            else System.out.println("failure");
         }
         else System.out.println("failure");
      }
      catch (SQLException e) {
         System.out.println("failure");
//         conn.rollback();
      }
   }

   void penalty(String ridernumber) throws SQLException {
      try {
         // output revoked if ridernumber already in table and remove 
         execute("SELECT ridernumber FROM penalty WHERE ridernumber = " + ridernumber, 2);
         if (resultint == 1) {
            execute("DELETE FROM penalty WHERE ridernumber = " + ridernumber, 0);
            System.out.println("revoked");
            conn.commit();
         }
         // if ridernumber not already in table:
         else {
            // if team doesnt already exist in table output informed, add to table
            execute("SELECT * FROM penalty WHERE team = (SELECT riderteam FROM riderdata WHERE ridernumber = " + ridernumber + ")", 2);
            if (resultint == 0) {
               execute("INSERT INTO penalty (ridernumber, team) VALUES (" + ridernumber + ", (SELECT riderteam FROM riderdata WHERE ridernumber = " + ridernumber + "))", 0);
               System.out.println("informed");
               conn.commit();
            }
         }
      }
      catch (SQLException e) {
//         conn.rollback();
      }
   }

   void printadjusted(String inputs) throws SQLException {
      try {

         String[] data = inputs.split(":");
         int ridernumber = Integer.parseInt(data[0]);
         int penalty = Integer.parseInt(data[1]);

         // check ridernumber is in penalty table
         execute("SELECT * FROM penalty WHERE ridernumber = " + ridernumber, 2);
         if (resultint == 1) {

            // check ridernumber is registered in riderdata
            execute("SELECT * FROM riderdata WHERE ridernumber = " + ridernumber, 2);
            if (resultint == 1) {

               execute("SELECT points FROM riderdata WHERE ridernumber = " + ridernumber, 1);
               int orig_points = 0;
               while (resultset.next()) orig_points = resultset.getInt(1);
               int new_points = orig_points - penalty;
               System.out.println(new_points);
               execute("UPDATE penalty SET new_points = " + new_points + " WHERE ridernumber = " + ridernumber, 0);
               conn.commit();
            }
         }
      }
      catch (SQLException e) {
//         conn.rollback();
      }
   }

   void setpoints(String inputs) throws SQLException {
      try {
         String[] data = inputs.split(":");
         int ridernumber = Integer.parseInt(data[0]);
         int new_points = Integer.parseInt(data[1]);

         // check rider currently in penalty table
         execute("SELECT * FROM penalty WHERE ridernumber = " + ridernumber, 2);
         if (resultint == 1) {

            // check new_points input matches expected (current_points - penalty)
            execute("SELECT new_points FROM penalty WHERE ridernumber = " + ridernumber, 1);
            int stored_new_points = 0;
            while (resultset.next()) stored_new_points = resultset.getInt(1);
            if (stored_new_points - new_points == 0) {
               execute("UPDATE riderdata SET points = " + new_points + " WHERE ridernumber = " + ridernumber, 0);
               execute("DELETE FROM penalty WHERE ridernumber = " + ridernumber, 0);
               conn.commit();
               System.out.println("success");
            }
            else System.out.println("failure");
         }
         else System.out.println("failure");
      }
      catch (SQLException e) {
//         conn.rollback();
         System.out.println("failure");
      }
   }


   void Switch(String[] args) throws SQLException {
      switch(Input.valueOf(args[0])) {
         case login: login(args[1]); break;
         case who: who(); break;
         case schema: schema(args[1]); break;
         case transponder: transponder(args[1]); break;
         case race: race(args[1]); break;
         case riderdata: riderdata(args[1]); break;
         case circuit: circuit(args[1]); break;
         case unit: unit(args[1]); break;
         case print: print(args[1]); break;
         case postrace: postrace(args[1]); break;
         case addpoints: conn.setAutoCommit(false); addpoints(args[1]); break;
         case penalty: conn.setAutoCommit(false); penalty(args[1]); break;
         case printadjusted: conn.setAutoCommit(false); printadjusted(args[1]); break;
         case setpoints: conn.setAutoCommit(false); setpoints(args[1]); break;
      }
   }

   public static void main(String[] args) throws SQLException {
      CWK3 MotoGP = new CWK3();
      try {
         Class.forName("oracle.jdbc.driver.OracleDriver");
      }
      catch(ClassNotFoundException e) {
      }
      try {
         String URL = "jdbc:oracle:thin:@danno.cs.bris.ac.uk:1521:teaching";
         conn = DriverManager.getConnection(URL, "mw7289", "brie");
         MotoGP.Switch(args);
         conn.close();
      }
      catch(Exception e) {
//      e.printStackTrace();
      }
   }

}

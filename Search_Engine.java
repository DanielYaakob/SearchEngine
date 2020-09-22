import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Search_Engine
{
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String CONNECT = "jdbc:mysql://localhost:3306/";

    public static void main(String[] args) throws SQLException, FileNotFoundException, ParseException
    {
        System.out.println("Connecting to database...");
        PreparedStatement prep;
        String line;
        String[] strp;
        int amount_ids = 0;
        int amount_recommends = 0;
        Connection connection;
        connection = DriverManager.getConnection(CONNECT, USERNAME, PASSWORD);
        System.out.println("Connected to database.");

        File file1 = new File("genres.csv");
        Scanner genresfile = new Scanner(file1);
        File file2 = new File("keywords.csv");
        Scanner keywordsfile = new Scanner(file2);
        File file3 = new File("productions.csv");
        Scanner productionsfile = new Scanner(file3);
        File file4 = new File("extra_info.csv");
        Scanner others = new Scanner(file4);

        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();

        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date javadate;
        java.sql.Date sqldate;

        System.out.println("Creating database 'search_engine'...");
        statement.executeUpdate("DROP DATABASE IF EXISTS search_engine");
        statement.executeUpdate("CREATE DATABASE IF NOT EXISTS search_engine");
        statement.executeQuery("USE search_engine");

        System.out.println("Creating tables for database...");

        //creating tables
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS genres" +
                "(" +
                "movie_id INT, " +
                "genre_id INT" +
                ");");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS keywords" +
                "(" +
                "movie_id INT, " +
                "keyword_id INT" +
                ");");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS productions" +
                "(" +
                "movie_id INT, " +
                "productions_id INT" +
                ");");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS others" +
                "(" +
                "movie_id INT, " +
                "movie_title VARCHAR(255), " +
                "popularity FLOAT, " +
                "release_date DATE, " +
                "revenue LONG, " +
                "vote_avg FLOAT" +
                ");");

        System.out.println("Inserting into genres table...");
        //inserting into genres
        String into_players = "INSERT INTO genres values(?,?);";

        prep = connection.prepareStatement(into_players);

        while(genresfile.hasNextLine())
        {
            line = genresfile.nextLine();
            strp = line.split(",");

            int i;
            for(i = 0; i < strp.length; i++)
            {
                strp[i] = strp[i].replaceAll("^\"|\"$", "");
            }

            prep.setInt(1, Integer.parseInt(strp[0]));
            prep.setInt(2, Integer.parseInt(strp[1]));

            prep.addBatch();

        }

        prep.executeBatch();
        connection.commit();
        genresfile.close();
        System.out.println("Insert successful");

        System.out.println("Inserting into keywords table...");
        //inserting into keywords
        String into_teams = "INSERT INTO keywords values(?,?);";

        prep = connection.prepareStatement(into_teams);

        while(keywordsfile.hasNextLine())
        {
            line = keywordsfile.nextLine();
            strp = line.split(",");

            int i;
            for(i = 0; i < strp.length; i++)
            {
                strp[i] = strp[i].replaceAll("^\"|\"$", "");
            }

            prep.setInt(1, Integer.parseInt(strp[0]));
            prep.setInt(2, Integer.parseInt(strp[1]));

            prep.addBatch();

        }

        prep.executeBatch();
        connection.commit();
        keywordsfile.close();
        System.out.println("Insert successful");

        System.out.println("Inserting into others table...");
        //inserting into others
        String other = "INSERT INTO others values(?,?,?,?,?,?);";

        prep = connection.prepareStatement(other);

        while(others.hasNextLine())
        {
            line = others.nextLine();
            strp = line.split(",");

            int i;
            for(i = 0; i < strp.length; i++)
            {
                strp[i] = strp[i].replaceAll("^\"|\"$", "");
            }

            javadate = date.parse(strp[3]);
            sqldate = new java.sql.Date(javadate.getTime());

            prep.setInt(1, Integer.parseInt(strp[0]));
            prep.setString(2, strp[1]);
            prep.setFloat(3, Float.parseFloat(strp[2]));
            prep.setDate(4, sqldate);
            prep.setLong(5, Long.parseLong(strp[4]));
            prep.setFloat(6, Float.parseFloat(strp[5]));

            prep.addBatch();

        }

        prep.executeBatch();
        connection.commit();
        others.close();
        System.out.println("Insert successful");

        System.out.println("Inserting into productions table...");
        //inserting into productions
        String into_members = "INSERT INTO productions values(?,?);";

        prep = connection.prepareStatement(into_members);

        while(productionsfile.hasNextLine())
        {
            line = productionsfile.nextLine();
            strp = line.split(",");

            int i;
            for(i = 0; i < strp.length; i++)
            {
                strp[i] = strp[i].replaceAll("^\"|\"$", "");
            }

            prep.setInt(1, Integer.parseInt(strp[0]));
            prep.setInt(2, Integer.parseInt(strp[1]));

            prep.addBatch();

        }

        prep.executeBatch();
        connection.commit();
        productionsfile.close();
        System.out.println("Insert successful");

        boolean correct = false;
        while(!correct)
        {
            System.out.print("Enter amount of recommendations you would for each category:");
            Scanner num = new Scanner(System.in);

            if (num.hasNextInt())
            {
                amount_recommends = num.nextInt();
                amount_recommends = amount_recommends + 2;
                correct = true;
            }

            else
                System.out.println("Please enter an only integer values, Try again...");
        }

        //queries

        while(true)
        {
            System.out.print("Enter a movie title (type 'quit' to quit):");
            Scanner scanner = new Scanner(System.in);
            ResultSet result;
            String userinput;

            userinput = scanner.nextLine();

            if(userinput.equals("quit"))
            {
                System.exit(0);
            }

            result = statement.executeQuery("SELECT others.movie_title FROM others WHERE others.movie_title = '" + " " + userinput + "';");
            boolean ree = result.next();

            if(!ree)
            {
                System.out.println("Movie does not exist.");
            }
            else
            {
                System.out.println("MOVIE ENTERED:");
                System.out.println(result.getString("others.movie_title"));

                //GETTING RECOMMENDED GENRES
                result = statement.executeQuery("SELECT genre_id FROM others, genres WHERE others.movie_id = genres.movie_id AND others.movie_title = '" + " " + userinput + "';");

                while(result.next())
                {
                    amount_ids++;
                }

                ArrayList<Integer> ids = new ArrayList<>(amount_ids);

                result = statement.executeQuery("SELECT genre_id FROM others, genres WHERE others.movie_id = genres.movie_id AND others.movie_title = '" + " " + userinput + "';");

                while(result.next())
                {
                    ids.add(result.getInt("genres.genre_id"));
                }

                result = statement.executeQuery("SELECT movie_title FROM others, genres WHERE others.movie_id = genres.movie_id AND genres.genre_id = " + ids.get(0) + " LIMIT " + amount_recommends + ";");

                System.out.println("RECOMMENDED MOVIES BASED ON GENRES:");
                boolean first = true;
                while(result.next())
                {
                    if(first)
                    {
                        result.next();
                        first = false;
                    }
                    else
                        System.out.println(result.getString("others.movie_title"));
                }

                amount_ids = 0;

                //GETTING RECOMMENDED KEYWORDS
                result = statement.executeQuery("SELECT genre_id FROM others, genres WHERE others.movie_id = genres.movie_id AND others.movie_title = '" + " " + userinput + "';");

                while(result.next())
                {
                    amount_ids++;
                }

                ArrayList<Integer> keywords = new ArrayList<>(amount_ids);

                result = statement.executeQuery("SELECT keyword_id FROM others, keywords WHERE others.movie_id = keywords.movie_id AND others.movie_title = '" + " " + userinput + "';");

                while(result.next())
                {
                    keywords.add(result.getInt("keywords.keyword_id"));
                }

                result = statement.executeQuery("SELECT movie_title FROM others, keywords WHERE others.movie_id = keywords.movie_id AND keywords.keyword_id = " + keywords.get(0) + " LIMIT " + amount_recommends + ";");

                System.out.println("RECOMMENDED MOVIES BASED ON KEYWORDS:");
                first = true;
                while(result.next())
                {
                    if(first)
                    {
                        result.next();
                        first = false;
                    }
                    else
                        System.out.println(result.getString("others.movie_title"));
                }

                amount_ids = 0;

                //GETTING RECOMMENDED PRODUCTIONS
                result = statement.executeQuery("SELECT productions_id FROM others, productions WHERE others.movie_id = productions.movie_id AND others.movie_title = '" + " " + userinput + "';");

                while(result.next())
                {
                    amount_ids++;
                }

                if(amount_ids == 0)
                {
                    System.out.println("RECOMMENDED MOVIES BASED ON PRODUCTION COMPANIES:");
                    System.out.println("(" + userinput + " has no production company)");
                }

                else
                {
                    ArrayList<Integer> productions = new ArrayList<>(amount_ids);

                    result = statement.executeQuery("SELECT productions_id FROM others, productions WHERE others.movie_id = productions.movie_id AND others.movie_title = '" + " " + userinput + "';");

                    while (result.next()) {
                        productions.add(result.getInt("productions.productions_id"));
                    }

                    result = statement.executeQuery("SELECT movie_title FROM others, productions WHERE others.movie_id = productions.movie_id AND productions.productions_id = " + productions.get(0) + " LIMIT " + amount_recommends + ";");

                    System.out.println("RECOMMENDED MOVIES BASED ON PRODUCTION COMPANIES:");
                    first = true;
                    while (result.next()) {
                        if (first) {
                            result.next();
                            first = false;
                        } else
                            System.out.println(result.getString("others.movie_title"));
                    }

                    amount_ids = 0;
                }

                System.out.println();
            }
        }
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab5;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 *
 * @author shusnain.bscs13seecs
 */
public class Lab5 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
       
         
          /**
     * @param args the command line arguments
     */
    public static ResultSetMetaData rsmd;
    public static Connection conn;
    public static ArrayList<String> fileData;

    public static void main(String[] args) {
        // TODO code application logic here

        // read file
        
        fileData = new ArrayList<String>();
        
        File file = new File("GeoLiteCity-Location.csv");
        FileReader fileReader;
        try {
            fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);
            
            StringBuffer stringBuffer = new StringBuffer();
            String line;
            
            try {
                while((line = br.readLine()) != null){
                    
                    stringBuffer.append(line);
                    stringBuffer.append("\n");
                    fileData.add(line);
                    
                }
                
                fileReader.close();
                System.out.println("Contents: ");
               
                
            } catch (IOException ex) {
                Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        

        conn = null;
        rsmd = null;
        
        

        initialization();
        normalStatement(conn);   

    }

    public static void initialization() {

        try {
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            
            System.out.println(ex.toString());
        }

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/GEOLITECITY?" + "user=root&password=root");
            
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void normalStatement(Connection conn) {

        try {

            Statement statement = conn.createStatement();
            
            int readSize = 1002;
            for (int i = 2; i < readSize; i++) {
                
                List<String> tokens = Arrays.asList(fileData.get(i).split("," , -1));
                
                System.out.println(tokens.size());
                
                for (int j = 0; j < tokens.size(); j++) {
                    if (tokens.get(j).length() == 0){
                    
                        
                        tokens.set(j, "NULL");
                    }
                }
                
                
                
                int locId = Integer.parseInt(tokens.get(0));
                
                String country = tokens.get(1);
                String region= tokens.get(2);
                String city = tokens.get(3);
                
                String postalCode = tokens.get(4);
                String latitude = tokens.get(5);
                String longitude = tokens.get(6);
               
                
                String metroCode = tokens.get(7);
                String areaCode = tokens.get(8);
                
                String sqlQuery = "INSERT INTO city_inf VALUES(" + locId + ",\"" + country + "\",\"" + region + "\",\"" + city + "\",\"" + postalCode + "\",\"" + latitude + "\",\"" + longitude + "\",\"" + metroCode + "\",\"" + areaCode + "\")" ;
                statement.execute(sqlQuery);
                
    
                
                System.out.print(locId + ",");
                System.out.print(country + ",");
                System.out.print(region + ",");
                System.out.print(city + ",");
                System.out.print(postalCode+ ",");
                System.out.print(latitude+ ",");
                System.out.print(longitude+ ",");
                System.out.print(metroCode+ ",");
                System.out.print(areaCode);
                System.out.println("");
            }
            
//            String sqlQuery = "INSERT INTO city_inf VALUES(" + locId + "," + country ;
//            ResultSet rs = statement.executeQuery(sqlQuery);
//
//            rsmd = rs.getMetaData(); // getColumnCount(), getColumnName(1) , rsmd.getColumnTypeName(1)  
//
//            printDatabaseMetaData(conn);
//
//            int columnCount = rsmd.getColumnCount();
//            System.out.println("# of columns: " + columnCount);
//            System.out.print("Column names: ");
//            for (int i = 1; i <= columnCount; i++) {
//
//                System.out.print("|" + rsmd.getColumnName(i) + "| ");
//
//            }
//
//            System.out.println("");
//
//            // int, String, int, String, int, double, double, int, int
//            while (rs.next()) {
//
//                int id = rs.getInt(1);
//                String firstName = rs.getString(2);
//                String lastName = rs.getString(3);
//                //Date date = rs.getDate(4); // this truncates timestamp to date only.
//                Timestamp ts = rs.getTimestamp(4);
//
//                System.out.println("ID: " + id + " FN: " + firstName + " LN: " + lastName + " TImestamp: " + ts);
//            }

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

    }

    public static void preparedStatement(Connection conn) {

        try {
            String sqlModificationQuery = "Insert into actor(actor_id, first_name, last_name, last_update) values(?,?,?,?);";

            PreparedStatement prep_statement = conn.prepareStatement(sqlModificationQuery);

            prep_statement.setInt(1, 201);
            prep_statement.setString(2, "Mohammad");
            prep_statement.setString(3, "Ahmad");

            Calendar calendar = Calendar.getInstance();
            java.util.Date now = calendar.getTime();
            java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());

            prep_statement.setTimestamp(4, currentTimestamp);

            int affectedRows = prep_statement.executeUpdate();
            System.out.println("Executed prepared statement. Affected rows: " + affectedRows);

        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void callableStatement(Connection conn) {

        try {
            CallableStatement callableStatement = conn.prepareCall("{call simpleproc()}",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);

            String param1 = null;
            

            ResultSet rs = callableStatement.executeQuery(); // if returns ResultSet
            

            int count = 0;
            while (rs.next()) {

                count++;

                int id = rs.getInt(1);
                String firstName = rs.getString(2);
                String lastName = rs.getString(3);
                
                Timestamp ts = rs.getTimestamp(4);

                System.out.println("ID: " + id + " FN: " + firstName + " LN: " + lastName + " TImestamp: " + ts);

            }

            System.out.println("ResultSet had rows: " + count);


        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void printDatabaseMetaData(Connection conn) {

        try {

            DatabaseMetaData dbmd = conn.getMetaData();

            System.out.println("Driver Name: " + dbmd.getDriverName());
            System.out.println("Driver Version: " + dbmd.getDriverVersion());
            System.out.println("UserName: " + dbmd.getUserName());
            System.out.println("Database Product Name: " + dbmd.getDatabaseProductName());
            System.out.println("Database Product Version: " + dbmd.getDatabaseProductVersion());
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
        
    
    }
}

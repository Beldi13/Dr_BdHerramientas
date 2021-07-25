package bdherramienta;

import java.awt.Component;
import java.awt.HeadlessException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;

/**
 * <h1>PostgresDb</h1>
 * Esta clase te permitirá establecer una conexion a una base de datos en
 * postgresql, ademas de poder ejecutar SQLs SELECT,INSERT,UPDATE y DELETE.
 * También puede generar un modelo de tabla, precargada con los datos de un
 * SELECT que especifiques. Podrás terminar procesos, restaurar y respaldar la
 * base de datos.
 *
 * @author Diego Robles
 * @version 2.0
 * @since 21/07/2021
 *
 * <h2>Beldi</h2>
 *
 * Youtube: https://www.youtube.com/channel/UCZlfwXOa8je8NzAnBJDtqzA
 * <br>
 * Facebook: https://www.facebook.com/beldi13
 * <br>
 * Omlet: https://omlet.gg/profile/beldi13
 * <br>
 * Twitch: https://www.twitch.tv/beldi133
 */
public class PostgresDb {

    /**
     * Variable estatica para definir el tipo de comando RESTAURAR en el metodo
     * ejecutarComando.
     */
    public static final String RESTAURAR = "restaurar";

    /**
     * Variable estatica para definir el tipo de comando REPALDAR en el metodo
     * ejecutarComando.
     */
    public static final String RESPALDAR = "respaldar";
    private final String direccion;
    private final String puerto;
    private final String nombreBD;
    private final String usuarioBD;
    private final String clavePG;
    private boolean estado = false;
    private Connection conex;
    private PreparedStatement ps;
    private ResultSet rs;
    private SimpleDateFormat fecha = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Construye una instancia bajo los parámetros indicados
     *
     * @param direccion. El host donde esta la base de datos.
     * @param puerto. El puerto que usa la base de datos.
     * @param nombreBD. El nombre de la base de datos.
     * @param usuarioBD. El usuario de la base de datos.
     * @param clavePG. La contraseña master de postgres que le asignaste.
     */
    public PostgresDb(String direccion, String puerto, String nombreBD, String usuarioBD, String clavePG) {
        this.direccion = direccion;
        this.puerto = puerto;
        this.nombreBD = nombreBD;
        this.usuarioBD = usuarioBD;
        this.clavePG = clavePG;
    }

    /**
     * Este método realiza la conexión a la base de datos.
     */
    private Connection iniciarConexion() {
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://" + direccion + ":" + puerto + "/" + nombreBD;
            conex = DriverManager.getConnection(url, usuarioBD, clavePG);
        } catch (HeadlessException | ClassNotFoundException | SQLException e) {
            System.out.println("Error iniciarConexion: " + e);
        }
        return conex;
    }

    /**
     * Método para cerrar la conexión a la base de datos.
     */
    private void cerrarConexion() {
        try {
            conex.close();
        } catch (SQLException e) {
            System.out.println("Error cerrarConexion: " + e);
        }
    }

    /**
     * Retorna la conexión establecida
     *
     * @return Retorna la conexión de tipo Connection
     */
    public Connection obtenerConexion() {
        return conex;
    }

    /**
     * Uso de isValid
     *
     * @return true si es valida y falso en cualquier otro caso.
     */
    public boolean validarConexion() {
        try {
            estado = iniciarConexion().isValid(50000);
        } catch (SQLException e) {
            estado = false;
            System.out.println("Error conecionValida: " + e);
        }
        cerrarConexion();
        return estado;
    }

    /**
     * Método para ejecutar SQLs(IUD): Insert, Update y Delete.
     *
     * @param sql. Parámetro que recibe el SQL a ejecutar.
     * @return true si se ejecuta el sql y false en cualquier otro caso.
     */
    public boolean ejecutarIUDsql(String sql) {
        iniciarConexion();
        try {
            ps = conex.prepareStatement(sql);
            ps.execute();
            ps.close();
            estado = true;
        } catch (SQLException e) {
            estado = false;
            System.out.println("Error ejecutarIUDsql: " + e);
        }
        cerrarConexion();
        return estado;
    }

    /**
     * Método que ejecuta un SQL Select y devuelve los datos en un ResultSet.
     *
     * @param sql Parámetro que contiene el SQL a ejecutar.
     * @return Retornar los datos del select en tipo ResultSet.
     */
    public ResultSet ejecutarSelectSql(String sql) {
        iniciarConexion();
        try {
            ps = conex.prepareStatement(sql);
            rs = ps.executeQuery();
        } catch (SQLException e) {
            System.out.println("Error ejecutarSelectSql: " + e);
        }
        cerrarConexion();
        return rs;
    }

    /**
     * Método para ejecutar un sql select a modo de verificación. Si encuentra
     * almenos un registro devolvera true y falso en cualquier otro caso.
     *
     * @param sql Parámetro que contiene el SQL a ejecutar.
     * @return Retorna true y falso en cualquier otro caso.
     */
    public boolean SiNoRegistro(String sql) {
        iniciarConexion();
        try {
            if (ejecutarSelectSql(sql).isBeforeFirst()) {
                estado = true;
            }
        } catch (SQLException e) {
            estado = false;
            System.out.println("Error SiNoRegistro: " + e);
        }
        cerrarConexion();
        return estado;
    }

    /**
     * Método genérico para crear un modelo de tabla cargado con los datos de la
     * consulta SELECT. El modelo puede ser asignado a una tabla vacía para
     * llenarla.
     *
     * @param camposEditables true si los campos seran editables y false para no
     * editables.
     * @param sql. Parámetro que contiene el SQL a ejecutar.
     * @return Retorna un modelo con datos de tipo DefaulttTableModel.
     */
    public DefaultTableModel crearModeloDeTabla(boolean camposEditables, String sql) {
        DefaultTableModel model = new DefaultTableModel();
        ResultSet data = ejecutarSelectSql(sql);
        try {
            ResultSetMetaData metaData = data.getMetaData();
            Object[] rows = new Object[metaData.getColumnCount()];
            Object[] columnsName = new Object[metaData.getColumnCount()];
            int Ncolumnas = metaData.getColumnCount();
            if (!camposEditables) {
                model = new DefaultTableModel() {
                    @Override
                    public boolean isCellEditable(int row, int column) {
                        return column == Ncolumnas;
                    }
                };
            }
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columnsName[i - 1] = metaData.getColumnName(i);
            }
            model.setColumnIdentifiers(columnsName);
            while (data.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    rows[i - 1] = data.getString(i);
                }
                model.addRow(rows);
            }
        } catch (SQLException e) {
            System.out.println("Error crearModeloDeTabla: " + e);
        }
        return model;
    }

    /**
     * Método para terminar cualquier procesos que esté impidiendo respaldar o
     * restaurar la base de datos.
     *
     * @param componente Componente que se tomara como referencia para mostrar
     * un mensaje en el centro del mismo.
     */
    public void terminarProcesos(Component componente) {
        try {
            Class.forName("org.postgresql.Driver");
            conex = DriverManager.getConnection("jdbc:postgresql://" + direccion + ":" + puerto + "/", usuarioBD, clavePG);
            String sql = "SELECT pg_terminate_backend(pg_stat_activity.pid)\n"
                    + "FROM pg_stat_activity\n"
                    + "WHERE pg_stat_activity.datname = '" + nombreBD + "'\n"
                    + "AND pid <> pg_backend_pid();";
            ps = conex.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.isBeforeFirst()) {
                JOptionPane.showMessageDialog(componente, "Aún hay procesos por terminar, vuelve a hacer click.", "Información", JOptionPane.INFORMATION_MESSAGE);
            } else {
                JOptionPane.showMessageDialog(componente, "Procesos Terminados, ya puedes restaurar.", "Información", JOptionPane.INFORMATION_MESSAGE);
            }
            rs.close();
            ps.close();
            cerrarConexion();
        } catch (HeadlessException | ClassNotFoundException | SQLException e) {
            JOptionPane.showMessageDialog(componente, "Error terminando procesos: " + e, "Información", JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * Método para crear una base de datos, si existe en el tablespace, se
     * eliminara y creara una nueva, caso contrario se creara solamente
     *
     * @param componente Componente que se tomara como referencia para mostrar
     * un mensaje en el centro del mismo.
     */
    private void crearBd(Component componente) {
        try {
            Class.forName("org.postgresql.Driver");
            conex = DriverManager.getConnection("jdbc:postgresql://" + direccion + ":" + puerto + "/", usuarioBD, clavePG);
            ps = conex.prepareStatement("SELECT datname FROM pg_catalog.pg_database WHERE LOWER(datname) = LOWER(?)");
            ps.setString(1, nombreBD);
            rs = ps.executeQuery();
            if (rs.isBeforeFirst()) {
                ps = conex.prepareStatement("drop database \"" + nombreBD + "\";");
                ps.execute();
                ps = conex.prepareStatement("CREATE DATABASE " + nombreBD);
                ps.execute();
                JOptionPane.showMessageDialog(componente, " Base de datos creada", "Información", JOptionPane.INFORMATION_MESSAGE);
            } else {
                ps = conex.prepareStatement("CREATE DATABASE " + nombreBD);
                ps.execute();
                JOptionPane.showMessageDialog(componente, " Base de datos creada", "Información", JOptionPane.INFORMATION_MESSAGE);
            }
            rs.close();
            ps.close();
            conex.close();
        } catch (HeadlessException | ClassNotFoundException | SQLException ex) {
            JOptionPane.showMessageDialog(componente, "Error creando la base de datos: " + ex, "Información", JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * Método para obtner los comandos del Dump de postgresql/pgadmin necesarios
     * para respaldar o restaurar la base de datos.
     *
     * @param componente Componente que se tomara como referencia para mostrar
     * un mensaje en el centro del mismo.
     * @param tipo String respaldar o restaurar.
     * @param PgVersion version de tu PgAdmin.
     * @return Retorna una lista de comandos.
     */
    private List<String> obtenerComandoPG(Component componente, String tipo, String PgVersion) {
        ArrayList<String> commands = new ArrayList<>();
        JFileChooser SeleccionarDirectorio = new JFileChooser();
        String ruta;
        switch (tipo) {
            case RESPALDAR:
                SeleccionarDirectorio.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                SeleccionarDirectorio.setDialogTitle("Selecionar la ubicacion y da un nombre para el respaldo de la base de datos");
                SeleccionarDirectorio.setDialogType(2);
                SeleccionarDirectorio.setApproveButtonText("Guardar ");
                SeleccionarDirectorio.showSaveDialog(componente);
                if (JFileChooser.APPROVE_OPTION == 0) {
                    ruta = SeleccionarDirectorio.getSelectedFile().getAbsolutePath();
                }
                commands.add("C:\\Program Files\\PostgreSQL\\" + PgVersion + "\\bin\\pg_dump.exe");
                commands.add("-h"); //host de la bd
                commands.add(direccion);//direccion
                commands.add("-p"); //puerto
                commands.add(puerto);//5436
                commands.add("-U"); //conecta a una bd especifica
                commands.add(usuarioBD);//postgres
                commands.add("-F"); //formato de archivo de salida (personalizado, directorio, tar, texto sin formato (predeterminado))
                commands.add("c");
                commands.add("-b"); //incluye objetos mas grandes
                commands.add("-v"); //verbose modo
                commands.add("-f"); //archivo de salidad o nombre del directorio
                commands.add(ruta + "_respaldo_" + nombreBD + "_" + fecha.format(new Date()) + ".sql");
                commands.add("-d"); //nombre de la bd
                commands.add(nombreBD);
                break;
            case RESTAURAR:
                crearBd(componente);
                SeleccionarDirectorio.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
                SeleccionarDirectorio.setFileFilter(new FileNameExtensionFilter("BackUp files (*.sql, *.backup)", "sql", "backup"));
                SeleccionarDirectorio.setDialogTitle("Selecionar el backup");
                SeleccionarDirectorio.setDialogType(2);
                SeleccionarDirectorio.setApproveButtonText("Guardar ");
                SeleccionarDirectorio.showSaveDialog(componente);
                if (JFileChooser.APPROVE_OPTION == 0) {
                    ruta = SeleccionarDirectorio.getSelectedFile().getAbsolutePath();
                }
                commands.add("C:\\Program Files\\PostgreSQL\\" + PgVersion + "\\bin\\pg_restore.exe");
                commands.add("-h");
                commands.add(direccion);
                commands.add("-p");
                commands.add(puerto);
                commands.add("-U");
                commands.add(usuarioBD);
                commands.add("-d");
                commands.add(nombreBD);
                commands.add("-v");
                commands.add(ruta);
                break;
            default:
                return Collections.EMPTY_LIST;
        }
        return commands;
    }

    /**
     * Método para ejecutar los comandos de RESTAURAR O RESPALDAR.
     *
     * @param componente Componente que se tomara como referencia para mostrar
     * un mensaje en el centro del mismo.
     * @param tipo String respaldar o restaurar.
     * @param PgVersion version de tu PgAdmin.
     */
    public void ejecutarComando(Component componente, String tipo, String PgVersion) {
        List<String> commands = obtenerComandoPG(componente, tipo, PgVersion);
        if (!commands.isEmpty()) {
            try {
                ProcessBuilder pb = new ProcessBuilder(commands);
                pb.environment().put("PGPASSWORD", clavePG);
                Process process = pb.start();
                try (BufferedReader buf = new BufferedReader(
                        new InputStreamReader(process.getErrorStream()))) {
                    String line = buf.readLine();
                    while (line != null) {
                        line = buf.readLine();
                    }
                }
                process.waitFor();
                process.destroy();
                JOptionPane.showMessageDialog(componente, tipo + " completado con éxito", "Información", JOptionPane.INFORMATION_MESSAGE);
            } catch (IOException | InterruptedException ex) {
                JOptionPane.showMessageDialog(componente, "Error ejecutar comando: " + ex, "Información", JOptionPane.ERROR_MESSAGE);
            }
        } else {
            JOptionPane.showMessageDialog(componente, "Error: Parametro inválido ", "Información", JOptionPane.ERROR_MESSAGE);
        }
    }
}

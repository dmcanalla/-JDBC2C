package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;
import lsi.ubu.util.exceptions.SGBDError;
import lsi.ubu.util.exceptions.oracle.OracleSGBDErrorUtil;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException 
	{
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) 
		{
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}

		try 
		{
			con = pool.getConnection();
			String query = "";
			query += " select a.MATRICULA, a.ID_MODELO, b.PRECIO_CADA_DIA, b.CAPACIDAD_DEPOSITO, b.TIPO_COMBUSTIBLE, c.PRECIO_POR_LITRO ";
			query += " from vehiculos a ";
			query += " inner join modelos b ";
			query += " on a.ID_MODELO = b.ID_MODELO ";
			query += " inner join precio_combustible c ";
			query += " on b.TIPO_COMBUSTIBLE = c.TIPO_COMBUSTIBLE ";
			query += " WHERE a.MATRICULA = ? ";

			st = con.prepareStatement(query);
			st.setString(1, matricula);

			rs = st.executeQuery();
			if (!rs.next()) {
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
			}

			/*
			 * Ayudas con tipos de datos
			 * 
			 * Algunas de las columnas utilizan tipo numeric en SQL, lo que se traduce en
			 * BigDecimal para Java.
			 * 
			 * Convertir un entero en BigDecimal: new BigDecimal(diasDiff)
			 * 
			 * Sumar 2 BigDecimals: usar metodo "add" de la clase BigDecimal
			 * 
			 * Multiplicar 2 BigDecimals: usar metodo "multiply" de la clase BigDecimal
			 */
			
			BigDecimal precioCadaDia = rs.getBigDecimal("PRECIO_CADA_DIA");
			int capacidadDeposito = rs.getInt("CAPACIDAD_DEPOSITO");
			BigDecimal precioPorLitro = rs.getBigDecimal("PRECIO_POR_LITRO");
			int idModelo = rs.getInt("ID_MODELO");
			String tipoCombustible = rs.getString("TIPO_COMBUSTIBLE");

			BigDecimal diasComoBigDecimal = new BigDecimal(diasDiff);
			BigDecimal capacidadDepositoComoBigDecimal = new BigDecimal(capacidadDeposito);

			BigDecimal precioAlquiler = precioCadaDia.multiply(diasComoBigDecimal);
			BigDecimal precioCombustible = precioPorLitro.multiply(capacidadDepositoComoBigDecimal);

			BigDecimal precioTotal = precioAlquiler.add(precioCombustible);

			rs.close();
			st.close();

			/*
			 * Ayudas con tipos de datos
			 * 
			 * Paso de util.Date a sql.Date java.sql.Date sqlFechaIni = new
			 * java.sql.Date(sqlFechaIni.getTime());
			 */
			java.sql.Date sqlFechaIni = new java.sql.Date(fechaIni.getTime());
			java.sql.Date sqlFechaFin = null;

			/*
			 * Recuerda que hay casos donde la fecha fin es nula, por lo que se debe de
			 * calcular sumando los dias de alquiler (ver variable DIAS_DE_ALQUILER) a la
			 * fecha ini.
			 */
			if (fechaFin != null)
				sqlFechaFin = new java.sql.Date(fechaFin.getTime());
			else {
				// Sumar 4 dias a la fechaIni
				Calendar cal = Calendar.getInstance();
				cal.setTime(fechaIni);
				cal.add(Calendar.DAY_OF_YEAR, DIAS_DE_ALQUILER);

				sqlFechaFin = new java.sql.Date(cal.getTimeInMillis());
			}

			// Comprobación reserva solapada
			st = con.prepareStatement( "SELECT IDRESERVA FROM RESERVAS WHERE MATRICULA = ? AND FECHA_FIN >= ? and ? >= FECHA_INI");

			st.setString(1, matricula);
			st.setDate(2, sqlFechaIni);
			st.setDate(3, sqlFechaFin);

			rs = st.executeQuery();
			if (rs.next()) 
			{
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
			}

			rs.close();
			st.close();

			// Inserción reservas
			st = con.prepareStatement(
					"INSERT INTO RESERVAS (IDRESERVA, CLIENTE, MATRICULA ,FECHA_INI,FECHA_FIN) VALUES (seq_reservas.nextVal, ?,?,?,?)");

			st.setString(1, nifCliente);
			st.setString(2, matricula);
			st.setDate(3, sqlFechaIni);
			
			if (fechaFin != null) 
			{
				st.setDate(4, sqlFechaFin);
			} else {
				st.setNull(4, java.sql.Types.DATE);
			}

			st.executeUpdate();

			st.close();

			// sacar valor del identificador de factura
			st = con.prepareStatement("SELECT seq_num_fact.nextVal as valor from dual");

			rs = st.executeQuery();
			rs.next();

			int seqFac = rs.getInt("valor");

			rs.close();
			st.close();

			// insertar factura
			st = con.prepareStatement("INSERT INTO FACTURAS (NROFACTURA, CLIENTE, IMPORTE) VALUES (?, ?,?)");
			st.setInt(1, seqFac);
			st.setString(2, nifCliente);
			st.setBigDecimal(3, precioTotal);

			st.executeUpdate();

			st.close();

			// insertar lineas de factura
			st = con.prepareStatement("INSERT INTO LINEAS_FACTURA (NROFACTURA, CONCEPTO, IMPORTE) VALUES (?, ?,?)");

			st.setInt(1, seqFac);
			st.setString(2, diasDiff + " dias de alquiler, vehiculo modelo " + idModelo);
			st.setBigDecimal(3, precioAlquiler);

			st.executeUpdate();

			st.close();

			st = con.prepareStatement("INSERT INTO LINEAS_FACTURA (NROFACTURA, CONCEPTO, IMPORTE) VALUES (?, ?,?)");

			st.setInt(1, seqFac);
			st.setString(2, "Deposito lleno de " + capacidadDeposito + " litros de " + tipoCombustible);
			st.setBigDecimal(3, precioCombustible);

			st.executeUpdate();

			st.close();

			con.commit();

		} 
		catch (SQLException e)
		{
			// Completar por el alumno
			if (con != null) 
			{
				con.rollback();
			}

			LOGGER.debug(e.getMessage());

			/* Si ha violado la FK, el cliente tiene pedidos */
			if (new OracleSGBDErrorUtil().checkExceptionToCode(e, SGBDError.FK_VIOLATED)) 
			{
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}

			if (e instanceof AlquilerCochesException) {
				throw (AlquilerCochesException) e;
			}

			throw e;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}


	public void anular_alquiler(String idReserva, String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException 
	{
	/* A completar por el alumnado */
		// primero instanciar la búsqueda
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		
		//comprobación fechas
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) 
		{
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}
		
		try
		{
			// conexión física
			con = pool.getConnection();
			// registro de coche MATRICULA
		
			String query = "";
			query += " select a.MATRICULA, a.ID_MODELO, b.PRECIO_CADA_DIA, b.CAPACIDAD_DEPOSITO, b.TIPO_COMBUSTIBLE, c.PRECIO_POR_LITRO ";
			query += " from vehiculos a ";
			query += " inner join modelos b ";
			query += " on a.ID_MODELO = b.ID_MODELO ";
			query += " inner join precio_combustible c ";
			query += " on b.TIPO_COMBUSTIBLE = c.TIPO_COMBUSTIBLE ";
			query += " WHERE a.MATRICULA = ? ";
			
			ResultSet estadoVehiculo = null;
			st = con.prepareStatement(query);
			st.setString(1, matricula);
			estadoVehiculo = st.executeQuery();
			
			if (!estadoVehiculo.next() || estadoVehiculo.getRow()==0) {
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
			}
			st.close();
			estadoVehiculo.close();
			// registro nif 
			// cargar la sentencia de la conexión en st
			st=con.prepareStatement("Select NIF from clientes where NIF = ?");
			st.setString(1, nifCliente);
			rs = st.executeQuery();
			// en caso de no tener el valor, implica que no existe el valor en la lista que queremos buscar
			if(!rs.next())
			{
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}
			st.close();
			rs.close();
			
			// reserva inexistente, partimos de que ya se han creado, entonces todas las reservas deben estar ya correctamente referenciadas
			// dentro de la tabla
			st = con.prepareStatement("Select idReserva from reservas where idReserva = ?");
			st.setString(1,idReserva);
			rs = st.executeQuery();
			if(!rs.next())
			{
				throw new AlquilerCochesException(AlquilerCochesException.RESERVA_NO_EXIST);
			}
			st.close();
			rs.close();
			
			// vehiculo no disponible - la búsqueda será más corta si queremos encontrar primero un coche que SI exista porque es lo ideal
			st = con.prepareStatement( "SELECT IDRESERVA FROM RESERVAS WHERE MATRICULA = ? AND FECHA_FIN >= ? and ? >= FECHA_INI");
			java.sql.Date fechaIniciosql= new  java.sql.Date(fechaIni.getTime());
			java.sql.Date fechaFinsql= new  java.sql.Date(fechaFin.getTime());
			st.setString(1, matricula);
			st.setDate(2, fechaIniciosql);
			st.setDate(3, fechaFinsql);
			
			rs = st.executeQuery();
			if (!rs.next() || fechaIniciosql == null || fechaFinsql ==null) 
			{
			throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
			}
			rs.close();
			st.close();
			
			// establezco los dates para las fechas momentáneas
			Date fechaInicialComprobar= null;
			Date fechaFinalComprobar = null;
			st = con.prepareStatement("Select FECHA_FIN as final, FECHA_INI as incial from reservas where matricula = ?");
			st.setString(1, matricula);
			rs = st.executeQuery();
			fechaFinalComprobar= new java.sql.Date(rs.getDate("final").getDate());
			fechaInicialComprobar = new java.sql.Date(rs.getDate("inicial").getDate());
			
			if(fechaFinalComprobar == null || fechaInicialComprobar == null)
			{
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
			st.close(); 
			rs.close();
			// borrado efectivo
			String borrado = "";
			borrado += "select factura as eliminado from facturas f";
			borrado += " join clientes c ";
			borrado += "on f.cliente = c.NIF";
			borrado += "where c.NIF = ?";
			
			st=con.prepareStatement(borrado);
			st.setString(1,nifCliente);
			rs = st.executeQuery();
			BigDecimal facturaBorrada = rs.getBigDecimal("eliminado");
			st = con.prepareStatement(" delete ? from facturas");
			st.setBigDecimal(1, facturaBorrada);
			rs = st.executeQuery();
			con.commit();
			st.close();
			rs.close();
		}
		catch (SQLException e)
		{
			if (con != null) 
			{
				con.rollback();
			}
			LOGGER.debug(e.getMessage());

			/* Si ha violado la FK, el cliente tiene pedidos */
			if (new OracleSGBDErrorUtil().checkExceptionToCode(e, SGBDError.FK_VIOLATED)) 
			{
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}

			if (e instanceof AlquilerCochesException) {
				throw (AlquilerCochesException) e;
			}

			throw e;

		} 
		finally 
		{
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}
		
	}
}

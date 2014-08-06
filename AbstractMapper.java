package mappers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;

public abstract class AbstractMapper<T,K> {

	protected DataSource ds;
	/**
	 * Returns the table Name.
	 * @return String TableName.
	 */
	protected abstract String getTableName();

	protected abstract String[] getColumnNames();
	
	/**
	 * Serializes an Object given.
	 * @param object Object to serialize
	 * @return Components of the serialized object.
	 */
	protected abstract Object[] serializeObject(T object);

	/**
	 * Builds an Object from a ResultSet.
	 * @param rs ResultSet
	 * @return Object from the rs.
	 */
	protected abstract T buildObject(ResultSet rs) throws SQLException;

	/**
	 * Returns the key column names of the table.
	 * 
	 * @return String[] with the key column names.
	 */
	protected abstract String[] getKeyColumnNames();
	
	
	/**
	 * Serializes a primary Key
	 *
	 * @param key Key to divide
	 * @return Components of the K key.
	 */
	protected abstract Object[] serializeKey(K key);
	
	/**
	 * Returns the T object primary key. 
	 * 
	 * @param object Objet
	 * @return Primary Key.
	 */
	protected abstract K getKey(T object);

	public AbstractMapper(DataSource ds) {
		this.ds = ds;
	}

	protected List<T> findByConditions(QueryCondition[] conditions) {
		
		Connection con        = null;
		PreparedStatement pst = null;
		ResultSet rs          = null;
		List<T> result       = new ArrayList<T>();
		try {
			con = ds.getConnection();
			String[] columnNames = getColumnNames();
			String columnNamesWithCommas = StringUtils.join(columnNames, ", ");
			String[] cadenaInterrogacion = new String[conditions.length];
			
			for(int i=0; i<conditions.length; i++){
					cadenaInterrogacion[i] = conditions[i].getColumnName() +" "+ conditions[i].getOperator() + " ? ";
			}
			
			pst = con.prepareStatement(
					"SELECT " + columnNamesWithCommas + " FROM " + getTableName() +  
					" WHERE " + StringUtils.join(cadenaInterrogacion, " AND ")
					);
			
			for(int i = 0; i < conditions.length; i++)
				pst.setObject(i+1, conditions[i].getValue());
			
			rs = pst.executeQuery();
			
			while (rs.next()) {
				result.add(buildObject(rs));
			} 
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) rs.close();
				if (pst != null) pst.close();
				if (con != null) con.close();
			} catch (Exception e) {}
		}
		return result;
	}
	
	public List<T> findAll() {

		Connection con        = null;
		PreparedStatement pst = null;
		ResultSet rs          = null;
		List<T> result       = new ArrayList<T>();
		try {
			con = ds.getConnection();
			String[] columnNames = getColumnNames();
			String columnNamesWithCommas = StringUtils.join(columnNames, ", ");
			
			pst = con.prepareStatement(
					"SELECT " + columnNamesWithCommas + " FROM " + getTableName()
					);
			
			rs = pst.executeQuery();
			
			while (rs.next()) {
				result.add(buildObject(rs));
			} 
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) rs.close();
				if (pst != null) pst.close();
				if (con != null) con.close();
			} catch (Exception e) {}
		}
		return result;
	}

	protected int deleteByConditions(QueryCondition[] conditions){
		
		Connection con        = null;
		PreparedStatement pst = null;
		int rs          = 0;
		try {
			con = ds.getConnection();
			String tableName = getTableName();
			String[] cadenaInterrogacion = new String[conditions.length];
			
			for(int i=0; i<conditions.length; i++){
					cadenaInterrogacion[i] = conditions[i].getColumnName() +" "+ conditions[i].getOperator() + " ? ";
			}
			
			pst = con.prepareStatement(
							"DELETE FROM  " + tableName + " WHERE " + 
							StringUtils.join(cadenaInterrogacion, " AND ")
					);
			
			for(int i = 0; i < conditions.length; i++)
				pst.setObject(i+1, conditions[i].getValue());
			
			rs = pst.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (pst != null) pst.close();
				if (con != null) con.close();
			} catch (Exception e) {}
		}
		return rs;
		
	}
	
	public T findById(K ids) {
		String[] columnNames = getKeyColumnNames();
		QueryCondition[] query = new QueryCondition[columnNames.length];
		Object[] obj = serializeKey(ids);
		for(int i=0; i<columnNames.length; i++){
			query[i]=new QueryCondition(columnNames[i], Operator.EQ, obj[i]);
		}
		
		List<T> resultado = findByConditions(query);
		
		if(!resultado.isEmpty())
			return resultado.get(0);
		else
			return null;
		
	}
	
	public int deleteById(K ids){
		String[] columnNames = getKeyColumnNames();
		QueryCondition[] query = new QueryCondition[columnNames.length];
		Object[] obj = serializeKey(ids);
		for(int i=0; i<columnNames.length; i++){
			query[i]=new QueryCondition(columnNames[i], Operator.EQ, obj[i]);
		}
		
		int resultado = deleteByConditions(query);
		
		return resultado;
	}
	
	public int insertObject(T object) {
		Connection con        = null;
		PreparedStatement pst = null;
		int rs = -1;
		try {
			con = ds.getConnection();
			String[] columnNames = getColumnNames();
			String columnNamesWithCommas = StringUtils.join(columnNames, ", ");
			Object[] values = serializeObject(object);
			String[] conditions = new String[values.length];
			
			for(int i=0; i<columnNames.length; i++){
					conditions[i] = " ? ";
			}
			
			pst = con.prepareStatement(
					"INSERT INTO " + getTableName() + " ( "+ columnNamesWithCommas + " )" + " VALUES ( "  
					 + StringUtils.join(conditions, " , ") + " ) "
					);
			
			
			for(int i = 0; i < conditions.length; i++)
				pst.setObject(i+1, values[i]);
			
			rs = pst.executeUpdate();
			
			} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (pst != null) pst.close();
				if (con != null) con.close();
			} catch (Exception e) {}
		}
		return rs;
	}
	
	public void update(T object){
		Connection con        = null;
		PreparedStatement pst = null;
		try {
			con = ds.getConnection();
			String[] columnNames = getColumnNames();
			String[] cadenaInterrogacionSet = new String[columnNames.length];
			String[] keyColumnNames = getKeyColumnNames();
			
			Object[] obj = null;
			obj = serializeObject(object);
			
			K clavesValor = getKey(object);
			Object[] valorClaves =  serializeKey(clavesValor);
						
			String[] where = null;
			where = new String[keyColumnNames.length];
			
			for(int i=0; i<columnNames.length; i++){
				cadenaInterrogacionSet[i] = columnNames[i]  + " = ? ";
			}

			for(int i = 0; i<keyColumnNames.length; i++){
				where[i] = keyColumnNames[i] + " = '" + valorClaves[i].toString() + "' ";
			}
			
			pst = con.prepareStatement(
					" UPDATE " + getTableName() +  
					" SET " + StringUtils.join(cadenaInterrogacionSet, " , ") +
					" WHERE " + StringUtils.join(where, " AND ")
					);
			
			for(int cont = 0; cont < columnNames.length; cont++)
				pst.setObject(cont+1, obj[cont]);
			
			pst.executeUpdate();
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (pst != null) pst.close();
				if (con != null) con.close();
			} catch (Exception e) {}
		}
		
	}
	
	public DataSource getDataSource(){
		return this.ds;
	}

}

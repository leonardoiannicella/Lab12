package it.polito.tdp.rivers.db;

import java.util.LinkedList;
import java.util.List;

import it.polito.tdp.rivers.model.FiumeResult;
import it.polito.tdp.rivers.model.Flow;
import it.polito.tdp.rivers.model.River;
import it.polito.tdp.rivers.model.RiverIdMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class RiversDAO {

	public List<River> getAllRivers(RiverIdMap riverMap) {
		
		final String sql = "SELECT id, name FROM river";

		List<River> rivers = new LinkedList<River>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();

			while (res.next()) {
				rivers.add(riverMap.get(new River(res.getInt("id"), res.getString("name"))));
			}

			conn.close();
			
		} catch (SQLException e) {
			//e.printStackTrace();
			throw new RuntimeException("SQL Error");
		}

		return rivers;
	}

	public FiumeResult getDatiFiume(River river) {
		final String sql = "SELECT MIN(day) as dataPrima, MAX(day) as dataUltima, COUNT(*) as count, AVG(flow) as media " + 
				"FROM flow " + 
				"WHERE flow.river = ?";
		FiumeResult fr = null;

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, river.getId());
			ResultSet res = st.executeQuery();

			if (res.next()) {
				LocalDate dataPrima = res.getDate("dataPrima").toLocalDate();
				LocalDate dataUltima = res.getDate("dataUltima").toLocalDate();
				int misurazioni = res.getInt("count");
				float media = res.getFloat("media");
				fr = new FiumeResult(dataPrima, dataUltima, misurazioni, media);
			}
			conn.close();
			if(fr==null)
				throw new IllegalStateException("Fiume non presente");
			return fr;
			
		} catch (SQLException e) {
			//e.printStackTrace();
			throw new RuntimeException("SQL Error");
		
		}
	}

	public List<Flow> getFlowsFromRiver(River river, RiverIdMap RiverMap) {
		
		final String sql = "SELECT day, flow.flow as f, flow.river as r " + 
				"FROM flow " + 
				"WHERE flow.river = ?";

		List<Flow> flows = new LinkedList<Flow>();
	
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, river.getId());
			ResultSet res = st.executeQuery();
	
			while (res.next()) {
				River r = RiverMap.get(res.getInt("r"));
				flows.add(new Flow(res.getDate("day").toLocalDate(), res.getDouble("f"), r));
			}
	
			conn.close();
			
		} catch (SQLException e) {
			//e.printStackTrace();
			throw new RuntimeException("SQL Error");
		}
	
		return flows;
		}
}

package py.restful.managers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.json.JSONArray;
import org.json.JSONObject;

@Stateless
@LocalBean
public class DatosManager {

	// para las respuestas
	JSONObject resp;

	@PersistenceContext(unitName = "opendata")
	private EntityManager em;

	private JSONArray listToJson(List<String> claves, List<Object[]> lista) {
		JSONObject json;
		JSONArray jsonLista = new JSONArray();
		DecimalFormat df = new DecimalFormat("#.00"); 
		
		for (Object[] ob : lista) {
			json = new JSONObject();

			for (int i = 0; i < claves.size(); i++) {
				String clave = claves.get(i);
				Object valor = ob[i];
				json.put(clave, valor);
			}
			jsonLista.put(json);
		}
		// convertir lista a un objeto json
		// e = new JSONObject();
		// e.put("lista", eArr);
		return jsonLista;
	}

	@SuppressWarnings("unchecked")
	public JSONObject getRankingExportaciones(String anhomes) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select dt.anho_mes, de.ruc, de.razon_social, "
							+ "sum(fc.valor_fob_dolar) valor_fob_dolar, "
							+ "sum(fc.kilo_neto) kilo_neto from	fact_certificado_origen fc "
							+ "join dim_tiempo dt on fc.fecha_finiquito = dt.id	"
							+ "join dim_certificado dcert on fc.certificado = dcert.id	"
							+ "left join dim_empresa de on fc.empresa = de.id "
							+ "where dt.anho_mes like :anhomes "
							+ "and dcert.estado_codigo = 'F' "
							+ "group by dt.anho_mes,de.ruc, de.razon_social "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			q.setParameter("anhomes", anhomes);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho_mes");
			claves.add("ruc");
			claves.add("razon_social");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Ranking de exportaciones traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer ranking de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getRankingExportaciones() {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select de.ruc, de.razon_social, "
							+ "sum(fc.valor_fob_dolar) valor_fob_dolar, "
							+ "sum(fc.kilo_neto) kilo_neto from	fact_certificado_origen fc "
							+ "join dim_tiempo dt on fc.fecha_finiquito = dt.id	"
							+ "join dim_certificado dcert on fc.certificado = dcert.id	"
							+ "left join dim_empresa de on fc.empresa = de.id "
							+ "where dcert.estado_codigo = 'F' "
							+ "group by de.ruc, de.razon_social "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("ruc");
			claves.add("razon_social");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Ranking de exportaciones traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer ranking de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getRankingImportaciones(String anhomes) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select dt.anho_mes, de.ruc, de.razon_social,"
							+ "sum(fi.valor_fob_dolar) as valor_fob_dolar,"
							+ "sum(fi.cantidad) as kilo_neto "
							+ "from fact_importacion fi join dim_tiempo dt on fi.fecha_finiquito = dt.id "
							+ "left join dim_empresa de on fi.empresa = de.id "
							+ "where dt.anho_mes like :anhomes "
							+ "group by dt.anho_mes, de.ruc, de.razon_social "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			q.setParameter("anhomes", anhomes);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho_mes");
			claves.add("ruc");
			claves.add("razon_social");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Ranking de importaciones traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer ranking de importaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getRankingImportaciones() {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select de.ruc, de.razon_social,"
							+ "sum(fi.valor_fob_dolar) as valor_fob_dolar,"
							+ "sum(fi.cantidad) as kilo_neto "
							+ "from fact_importacion fi join dim_tiempo dt on fi.fecha_finiquito = dt.id "
							+ "left join dim_empresa de on fi.empresa = de.id "
							+ "group by de.ruc, de.razon_social "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("ruc");
			claves.add("razon_social");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Ranking de importaciones traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer ranking de importaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getRankingProductosImportados(String anhomes) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select dt.anho_mes, dp.categoria, dp.subcategoria, "
							+ "sum(fi.valor_fob_dolar) as valor_fob_dolar "
							+ "from fact_importacion fi join dim_tiempo dt on fi.fecha_finiquito = dt.id "
							+ "left join fact_programa_produccion fp on fi.programa_produccion = fp.id "
							+ "left join dim_producto dp on fp.materia_prima = dp.id "
							+ "where dt.anho_mes like :anhomes "
							+ "and dp.categoria is not null "
							+ "group by dt.anho_mes, dp.categoria, dp.subcategoria "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			q.setParameter("anhomes", anhomes);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho_mes");
			claves.add("categoria");
			claves.add("subcategoria");
			claves.add("valor_fob_dolar");
			resp.put("status", "0");
			resp.put("mensaje",
					"Ranking de productos importados traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje",
					"Error al traer ranking de productos importados");
			resp.put("objeto", "{}");
		}
		return resp;

	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getRankingProductosImportados() {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select dp.categoria, dp.subcategoria, "
							+ "sum(fi.valor_fob_dolar) as valor_fob_dolar "
							+ "from fact_importacion fi join dim_tiempo dt on fi.fecha_finiquito = dt.id "
							+ "left join fact_programa_produccion fp on fi.programa_produccion = fp.id "
							+ "left join dim_producto dp on fp.materia_prima = dp.id "
							+ "where dp.categoria is not null "
							+ "group by dp.categoria, dp.subcategoria "
							+ "order by valor_fob_dolar desc "
							+ "limit 10 offset 0");

			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("categoria");
			claves.add("subcategoria");
			claves.add("valor_fob_dolar");
			resp.put("status", "0");
			resp.put("mensaje",
					"Ranking de productos importados traido con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje",
					"Error al traer ranking de productos importados");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getExportacionesPorPais(String anhomesDesde,
			String anhomesHasta) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select p.pais_abreviado, sum(valor_fob_dolar) valor_fob_dolar, "
							+ "sum(kilo_neto) kilo_neto "
							+ "from fact_certificado_origen f, dim_tiempo t, dim_pais p "
							+ "where f.fecha_finiquito = t.id "
							+ "and f.pais_destino = p.id "
							+ "and t.anho_mes between :anhomesDesde and :anhomesHasta "
							+ "group by p.pais_abreviado "
							+ "order by p.pais_abreviado");

			q.setParameter("anhomesDesde", anhomesDesde);
			q.setParameter("anhomesHasta", anhomesHasta);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("pais_abreviado");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Lista de exportaciones traida con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer la lista de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getExportacionesPorCategoria(String anhomes) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select dt.anho_mes, dp.partida_codigo as partida, "
							+ "dp.categoria, dp.subcategoria, sum(fc.valor_fob_dolar) valor_fob_dolar, sum(fc.kilo_neto) kilo_neto "
							+ "from	fact_certificado_origen fc "
							+ "join dim_tiempo dt on fc.fecha_finiquito = dt.id "
							+ "join dim_certificado dcert on fc.certificado = dcert.id "
							+ "left join dim_producto dp on fc.producto = dp.id "
							+ "where dcert.estado_codigo = 'F' "
							+ "and dt.anho_mes like :anhomes "
							+ "group by dt.anho_mes, dp.partida_codigo, dp.categoria, dp.subcategoria "
							+ "order by dt.anho_mes desc, dp.categoria, dp.subcategoria");

			q.setParameter("anhomes", anhomes);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho_mes");
			claves.add("partida");
			claves.add("categoria");
			claves.add("subcategoria");
			claves.add("valor_fob_dolar");
			claves.add("kilo_neto");

			resp.put("status", "0");
			resp.put("mensaje", "Lista de exportaciones traida con éxito");
			resp.put("objeto", listToJson(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer la lista de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getExportacionesPorAnho(String anho) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select "
							+ "de.razon_social as comprador,"
							+ "dp.pais_abreviado as pais_destino,"
							+ "dp.pais_codigo2 as importador,"
							+ "cast('PARAGUAY' as character varying(100)) as pais_origen,"
							+ "cast('PY' as character varying(100)) as exportador,"
							+ "sum(fc.valor_fob_dolar) valor_fob_dolar "
							+ "from fact_certificado_origen fc, dim_tiempo dt,"
							+ "dim_empresa de, dim_pais dp "
							+ "where fc.fecha_finiquito = dt.id "
							+ "and fc.empresa = de.id "
							+ "and fc.pais_destino = dp.id "
							+ "and dt.anho_4 like :anho "
							+ "group by dt.anho_mes,de.razon_social,dp.pais_abreviado,dp.pais_codigo2 "
							+ "order by dt.anho_mes");

			q.setParameter("anho", anho);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("comprador");
			claves.add("pais_destino");
			claves.add("importador");
			claves.add("pais_origen");
			claves.add("exportador");
			claves.add("valor_fob_dolar");

			resp.put("status", "0");
			resp.put("mensaje", "Lista de exportaciones traida con éxito");
			resp.put("objeto", listToJsonImExportacion(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer la lista de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getExportacionesPorAnhoPaginado(String anho, int first, int pageSize,
			String sortField, String order, String filters) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			String query = "select "
					+ "dt.anho_4 as anho,"
					+ "dt.mes as mes, "
					+ "dt.mes_nombre as mes_nombre, "
					+ "de.razon_social as comprador,"
					+ "dp.pais_abreviado as pais_destino,"
					+ "dp.pais_codigo2 as importador,"
					+ "cast('PARAGUAY' as character varying(100)) as pais_origen,"
					+ "cast('PY' as character varying(100)) as exportador,"
					+ "sum(fc.valor_fob_dolar) valor_fob_dolar "
					+ "from fact_certificado_origen fc, dim_tiempo dt,"
					+ "dim_empresa de, dim_pais dp "
					+ "where fc.fecha_finiquito = dt.id "
					+ "and fc.empresa = de.id "
					+ "and dt.anho_4 like :anho "
					+ "and fc.pais_destino = dp.id ";

			String groupBy = "group by dt.anho_4, dt.mes, dt.mes_nombre, dt.anho_mes,de.razon_social,dp.pais_abreviado,dp.pais_codigo2 ";
			String where = "";
			if (filters != null && filters.length() > 2) {

				String[] listaFiltros = filters.split("xx");
				for (String filtro : listaFiltros) {
					String[] claveValor = filtro.split("--");
					String clave = claveValor[0];
					String valor = claveValor[1];
					where += " and ";
					
					if (clave.equals("anho"))
						clave = "dt.anho_4";
					else if (clave.equals("mes"))
						clave = "dt.mes";
					else if (clave.equals("mes_nombre"))
						clave = "dt.mes_nombre";
					else if (clave.equals("comprador"))
						clave = "de.razon_social";
					else if (clave.equals("pais_destino")
							|| clave.equals("importador"))
						clave = "dp.pais_abreviado";
					else if (clave.equals("pais_origen"))
						clave = "cast('PARAGUAY' as character varying(100))";
					else if (clave.equals("exportador"))
						clave = "cast('PY' as character varying(100))";
					else if (clave.equals("valor_fob_dolar"))
						clave = "sum(fc.valor_fob_dolar)";

					where += clave + " ilike '%" + valor + "%' ";
				}
			}
			query += where;
			query += groupBy;
			query += "order by " + sortField + " " + order + " limit "
					+ pageSize + " offset " + first;
			Query q = em.createNativeQuery(query);
			q.setParameter("anho", anho);
			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho");
			claves.add("mes");
			claves.add("mes_nombre");
			claves.add("comprador");
			claves.add("pais_destino");
			claves.add("importador");
			claves.add("pais_origen");
			claves.add("exportador");
			claves.add("valor_fob_dolar");

			resp.put("status", "0");
			resp.put("mensaje", "Lista de exportaciones traida con éxito");
			resp.put("objeto", listToJsonImExportacion(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer la lista de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	public JSONObject getTotalExportacionesPorAnho(String anho) {

		resp = new JSONObject();
		try {
			String query = "select count(*) from ( "
					+ "select 1 as total from fact_certificado_origen fc, dim_tiempo dt,dim_empresa de, dim_pais dp "
					+ "where fc.fecha_finiquito = dt.id  and fc.empresa = de.id  and fc.pais_destino = dp.id "
					+ "and dt.anho_4 like :anho "
					+ "group by dt.anho_mes,de.razon_social,dp.pais_abreviado,dp.pais_codigo2) as cc";

			Query q = em.createNativeQuery(query);
			q.setParameter("anho", "%"+anho+"%");
			BigInteger total = (BigInteger) q.getSingleResult();

			JSONObject json = new JSONObject();
			json.put("total", total);
			resp.put("status", "0");
			resp.put("mensaje", "Total de exportaciones traida con éxito");
			resp.put("objeto", json);
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer el total de exportaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	@SuppressWarnings("unchecked")
	public JSONObject getImportacionesPorAnho(String anho) {

		resp = new JSONObject();
		try {
			List<Object[]> datosList = new ArrayList<Object[]>();
			Query q = em
					.createNativeQuery("select "
							+ "dt.anho_4 as anho,"
							+ "dt.mes as mes, "
							+ "dt.mes_nombre as mes_nombre, "
							+ "de.razon_social as comprador,"
							+ "cast('PARAGUAY' as character varying(100)) as pais_destino,"
							+ "cast('PY' as character varying(100)) as importador,"
							+ "dp.pais_abreviado as pais_origen,"
							+ "dp.pais_codigo2 as exportador,"
							+ "sum(fc.valor_fob_dolar) valor_fob_dolar "
							+ "from fact_importacion fc, dim_tiempo dt, dim_empresa de, dim_pais dp "
							+ "where fc.fecha_finiquito = dt.id "
							+ "and fc.empresa = de.id "
							+ "and fc.pais_origen = dp.id "
							+ "and dt.anho_4 like :anho "
							+ "group by dt.anho_4, dt.mes, dt.mes_nombre, dt.anho_mes,de.razon_social,dp.pais_abreviado,dp.pais_codigo2 "
							+ "order by dt.anho_mes");

			q.setParameter("anho", anho);

			datosList = q.getResultList();

			List<String> claves = new ArrayList<String>();
			claves.add("anho");
			claves.add("mes");
			claves.add("mes_nombre");
			claves.add("comprador");
			claves.add("pais_destino");
			claves.add("importador");
			claves.add("pais_origen");
			claves.add("exportador");
			claves.add("valor_fob_dolar");
			resp.put("status", "0");
			resp.put("mensaje", "Lista de importaciones traida con éxito");
			resp.put("objeto", listToJsonImExportacion(claves, datosList));
		} catch (Exception e) {
			resp.put("status", "1");
			resp.put("mensaje", "Error al traer la lista de importaciones");
			resp.put("objeto", "{}");
		}
		return resp;

	}

	private JSONArray listToJsonImExportacion(List<String> claves,
			List<Object[]> lista) {
		JSONObject json;
		JSONArray jsonLista = new JSONArray();

		for (Object[] ob : lista) {
			json = new JSONObject();

			for (int i = 0; i < claves.size(); i++) {
				String clave = claves.get(i);
				Object valor = ob[i];
				if (valor == null) {
					json.put(clave, "KO");
				} else {
					json.put(clave, valor);
				}

			}
			jsonLista.put(json);
		}
		// convertir lista a un objeto json
		// e = new JSONObject();
		// e.put("lista", eArr);
		return jsonLista;
	}
}

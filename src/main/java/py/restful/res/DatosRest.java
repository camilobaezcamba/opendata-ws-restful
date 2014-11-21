package py.restful.res;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.json.JSONObject;

import py.restful.managers.DatosManager;

@Stateless
@Path("/")
@Produces({ "application/json" })
public class DatosRest {

	// inyeccion del manager que actua de controlador
	@EJB
	DatosManager em;

	@GET
	@Path("/ranking/exportacion")
	public Response getRankingExportaciones() {
		String resp;
		JSONObject respObj = em.getRankingExportaciones();
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/ranking/exportacion/{anhomes}")
	public Response getRankingExportaciones(@PathParam("anhomes") String anhomes) {
		String resp;
		JSONObject respObj = em.getRankingExportaciones(anhomes);
		resp = respObj.toString();
		return respuesta(resp);
	}
	
	@GET
	@Path("/ranking/producto/importados")
	public Response getRankingProductosImportados() {
		String resp;
		JSONObject respObj = em.getRankingProductosImportados();
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/ranking/producto/importados/{anhomes}")
	public Response getRankingProductosImportados(@PathParam("anhomes") String anhomes) {
		String resp;
		JSONObject respObj = em.getRankingProductosImportados(anhomes);
		resp = respObj.toString();
		return respuesta(resp);
	}
	
	@GET
	@Path("/ranking/importacion")
	public Response getRankingImportaciones() {
		String resp;
		JSONObject respObj = em.getRankingImportaciones();
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/ranking/importacion/{anhomes}")
	public Response getRankingImportaciones(@PathParam("anhomes") String anhomes) {
		String resp;
		JSONObject respObj = em.getRankingImportaciones(anhomes);
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/exportacionesPorPais")
	public Response getExportacionesPorPais() {
		String resp;
		Calendar cal = Calendar.getInstance();
		int anho = cal.get(Calendar.YEAR);
		int mes = cal.get(Calendar.MONTH);

		String anhomesHasta = String.valueOf(anho) + String.valueOf(mes);

		SimpleDateFormat sd = new SimpleDateFormat("yyyyMM");
		anhomesHasta = sd.format(new Date());
		JSONObject respObj = em.getExportacionesPorPais("200501", anhomesHasta);
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/exportacionesPorPais/{anhomesDesde}-{anhomesHasta}")
	public Response getExportacionesPorPais(
			@PathParam("anhomesDesde") String anhomesDesde,
			@PathParam("anhomesHasta") String anhomesHasta) {
		String resp;
		JSONObject respObj = em.getExportacionesPorPais(anhomesDesde,
				anhomesHasta);
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/exportacionesPorAnho")
	public Response getExportacionesPorAnho() {
		String resp;
		JSONObject respObj = em.getExportacionesPorAnho("%%");
		resp = respObj.toString();
		return respuesta(resp);
	}
	@GET
	@Path("/exportacionesPorAnho/{anho}")
	public Response getExportacionesPorAnho(@PathParam("anho") String anho) {
		String resp;
		JSONObject respObj = em.getExportacionesPorAnho(anho);
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/exportacionesPorAnho/paginado/")
	public Response getExportacionesPorAnhoPaginado(
			@QueryParam("anho") String anho,
			@QueryParam("first") int first,
			@QueryParam("pageSize") int pageSize, @QueryParam("sortField") String sortField,
			@QueryParam("order") String order, @QueryParam("filters") String filters) {
		String resp;
		JSONObject respObj = em.getExportacionesPorAnhoPaginado(anho, first, pageSize, sortField, order, filters);
		resp = respObj.toString();
		return respuesta(resp);
	}
	
	@GET
	@Path("/exportacionesPorAnho/paginado/total/{anho}")
	public Response getTotalExportacionesPorAnho(@PathParam("anho") String anho) {
		String resp;
		JSONObject respObj = em.getTotalExportacionesPorAnho(anho);
		resp = respObj.toString();
		return respuesta(resp);
	}
	
	@GET
	@Path("/importacionesPorAnho")
	public Response getImportacionesPorAnho() {
		String resp;
		JSONObject respObj = em.getImportacionesPorAnho("%%");
		resp = respObj.toString();
		return respuesta(resp);
	}
	@GET
	@Path("/importacionesPorAnho/{anho}")
	public Response getImportacionesPorAnho(@PathParam("anho") String anho) {
		String resp;
		JSONObject respObj = em.getImportacionesPorAnho(anho);
		resp = respObj.toString();
		return respuesta(resp);
	}

	@GET
	@Path("/exportacionesPorCategoria/{anhomes}")
	public Response getExportacionesPorCategoria(@PathParam("anhomes") String anhomes){
		String resp;
		JSONObject respObj = em.getExportacionesPorCategoria(anhomes);
		resp = respObj.toString();
		return respuesta(resp);
	}
	@GET
	@Path("/exportacionesPorCategoria")
	public Response getExportacionesPorCategoria(){
		String resp;
		JSONObject respObj = em.getExportacionesPorCategoria("%%");
		resp = respObj.toString();
		return respuesta(resp);
	}
	
	public Response respuesta(String resp) {
		return Response
				.ok()
				// 200
				.entity(resp)
				.header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods",
						"GET, POST, DELETE, PUT").build();
	}

	/*
	 * BufferedWriter w = null; try { w = new BufferedWriter(new
	 * FileWriter("/opt/sam.csv"));
	 * w.write(CDL.toString(respObj.getJSONArray("objeto"))); w.close(); } catch
	 * (IOException e) { e.printStackTrace(); }
	 */
	/*
	 * Una buena API REST debe intentar cumplir los siguientes requisitos
	 * (obviamente además debe ser rápida y escalable):
	 * 
	 * Estar bien documentada. En caso de errores notificar al programador de
	 * una forma clara y suficiente para que el pueda averiguar que ha pasado y
	 * si es culpa suya corregir el problema. Proporcionar un mecanismo de
	 * filtro o consulta de selección de resultados. Proporcionar un mecanismo
	 * de respuesta parcial, en donde el programador pueda indicar que campos de
	 * información desea obtener, normalmente para ahorrar ancho de banda e
	 * incrementar velocidad en consultas desde dispositivos con recursos
	 * limitados. Estar versionado, para no afectar en futuras versiones a
	 * aplicaciones que usen versiones más antiguas del API. Ofecer un mecanismo
	 * de selección del formato de los datos de salida (normalmente XML o JSON).
	 * En este tutorial cubriremos los puntos: 2, 3, 4 y 5.
	 * 
	 * El punto 1 está documentado pero en los comentarios del código fuente,
	 * faltaría una url o documento que podrían consultar los programadores).
	 * 
	 * Para conseguir el punto 6, puedes consultar el siguiente artículo Spring
	 * MVC. Servicios REST respondiendo en JSON o XML.
	 * http://www.adictosaltrabajo
	 * .com/tutoriales/tutoriales.php?pagina=spring-mvc-rest
	 */
}

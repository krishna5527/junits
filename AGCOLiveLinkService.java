package com.agco.customerportal.dam.core.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.lang.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agco.customerportal.dam.core.beans.LiveLinkBean;
import com.agco.customerportal.dam.core.connections.DatabaseConnection;
import com.agco.customerportal.dam.core.constants.CoreConstants;
import com.agco.customerportal.dam.core.exceptions.DCXDAMException;
import com.day.cq.dam.api.AssetManager;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Component(service = AGCOLiveLinkService.class, immediate = true)
public class AGCOLiveLinkService {
	private static final String DETAILS_API = "https://aem-sync.fendt.com/api/details/";
	private static final String FILE_API = "https://aem-sync.fendt.com/api/file/";
	private static final String UPLOADED_DOC_IDS_QUERY = "SELECT livelink_id FROM dam.live_link_migration WHERE meta_data_status= 'Completed' AND file_upload_status= 'Completed' ORDER BY livelink_id ASC";
	private static final String EXISTING_IDS_WITH_METADATA_QUERY = "SELECT livelink_id FROM dam.live_link_migration WHERE meta_data_status= 'Completed' AND file_upload_status= 'Incomplete' ORDER BY livelink_id ASC";

	private static final Logger LOG = LoggerFactory.getLogger(AGCOLiveLinkService.class);

	@Reference
	private DatabaseConnection databaseConnection;

	@Reference
	private ResourceResolverFactory resolverFactory;

	@Reference
	AGCODAMCustomAssetUploadService customAssetUploadService;

	public String fetchResponseByStatus(String final_URL) {

		String jsonResponse = getJsonResponse(final_URL);
		ArrayList<Integer> validDocumentIds = getValidLiveLinkIds(jsonResponse);
		ArrayList<Integer> alreadyUploadedDocIds = getQueryResults(UPLOADED_DOC_IDS_QUERY);
		validDocumentIds.removeAll(alreadyUploadedDocIds);
		if (alreadyUploadedDocIds.size() == 0)
			insertMetaDataAndFileUploadStatus(validDocumentIds);
		ArrayList<Integer> existingDocIdsWithMetadata = getQueryResults(EXISTING_IDS_WITH_METADATA_QUERY);
		validDocumentIds.removeAll(existingDocIdsWithMetadata);
		insertMetadata(validDocumentIds);
		ArrayList<Integer> uploadedDocIds = uploadFileAssets();

		return "Documents are uploaded with below LiveLink Ids: \n" + uploadedDocIds.toString();
	}

	public ArrayList<Integer> getValidLiveLinkIds(String jsonResponse) {

		ArrayList<Integer> documentIds = new ArrayList<>();
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = parser.parse(jsonResponse);
		JsonArray jsonArray = jsonElement.getAsJsonArray();
		LOG.debug("Total No. Of Valid Documents: " + jsonArray.size());

		for (int i = 0, len = jsonArray.size(); i < len; i++)
			documentIds.add(Integer.parseInt(jsonArray.get(i).toString().replaceAll("\"", "")));

		return documentIds;
	}

	public ArrayList<Integer> getQueryResults(String query) {

		ArrayList<Integer> docIds = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		Connection connection = null;
		try {
			connection = databaseConnection.getConnection();
			preparedStatement = connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
				docIds.add(resultSet.getInt("livelink_id"));
		} catch (SQLException e) {
			LOG.error("SQL Excpetion thrown in getQueryResults method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (DCXDAMException e) {
			LOG.error("DCXDAM Excpetion thrown in getQueryResults method :: AGCOLiveLinkService: {}", e.getMessage());
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOG.error("Excpetion thrown in getQueryResults method in finally block:: AGCOLiveLinkService: {}", e.getMessage());
			}
		}

		return docIds;
	}

	public void insertMetaDataAndFileUploadStatus(ArrayList<Integer> liveLinkIds) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = databaseConnection.getConnection();
			for (int i = 0; i < liveLinkIds.size(); i++) {
				if (null != liveLinkIds.get(i)) {
					int executeUpdate = 0;
					preparedStatement = connection.prepareStatement("INSERT INTO dam.live_link_migration (livelink_id, meta_data_status, file_upload_status) VALUES (?,?,?)");
					preparedStatement.setInt(1, liveLinkIds.get(i));
					preparedStatement.setString(2, "Incomplete");
					preparedStatement.setString(3, "Incomplete");
					executeUpdate = preparedStatement.executeUpdate();
					if (executeUpdate == 0) {
						LOG.error("Insertion failed in DB for LiveLink ID: " + liveLinkIds.get(i));
					} else {
						LOG.debug("Insertion successful in DB for LiveLink ID: " + liveLinkIds.get(i));
					}
				}
			}
		} catch (SQLException e) {
			LOG.error("SQL Excpetion thrown in insertMetaDataAndFileUploadStatus method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (DCXDAMException e) {
			LOG.error("DCXDAM Excpetion thrown in insertMetaDataAndFileUploadStatus method :: AGCOLiveLinkService: {}", e.getMessage());
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOG.error("Excpetion thrown in insertMetaDataAndFileUploadStatus method in finally block:: AGCOLiveLinkService: {}", e.getMessage());
			}

		}
	}

	public void insertMetadata(ArrayList<Integer> documentIds) {

		PreparedStatement preparedStatement = null;
		Connection connection = null;
		JsonParser parser = new JsonParser();
		try {
			connection = databaseConnection.getConnection();
			for (int i = 0, len = documentIds.size(); i < len; i++) {
				String nodeDetailAPI = String.format("%s%s", DETAILS_API, documentIds.get(i));
				String detailsResponse = getJsonResponse(nodeDetailAPI);
				if (null != detailsResponse && StringUtils.isNotEmpty(detailsResponse)) {
					JsonElement jsonElement = parser.parse(detailsResponse);
					JsonObject jsonObject = jsonElement.getAsJsonObject();
					LiveLinkBean bean = new LiveLinkBean();
					bean.setType(jsonObject.get("type").toString().replaceAll("\"", ""));
					bean.setName(jsonObject.get("name").toString().replaceAll("\"", ""));
					bean.setFileName(jsonObject.get("filename").toString().replaceAll("\"", ""));
					bean.setParent(Integer.parseInt(jsonObject.get("parent").toString().replaceAll("\"", "")));
					bean.setSourceSystem(jsonObject.get("sourceSystem").toString().replaceAll("\"", ""));
					bean.setRootDAMPath(jsonObject.get("rootDAMPath").toString().replaceAll("\"", ""));
					bean.setExtendedPath(jsonObject.get("extendedPath").toString().replaceAll("\"", ""));
					if (jsonObject.has("metadata")) {
						JsonObject metaDataObj = jsonObject.getAsJsonObject("metadata");
						bean.setDcTitle(metaDataObj.get("dc:title").toString().replaceAll("\"", ""));
						bean.setDcDescription(metaDataObj.get("dc:description").toString().replaceAll("\"", ""));
						bean.setDcCategory(metaDataObj.get("dc:category").toString().replaceAll("\"", ""));
						bean.setCqTags(metaDataObj.get("cq:tags").toString().replaceAll("\"", ""));
						bean.setDcCategoryValue(metaDataObj.get("dc:categoryValue").toString().replaceAll("\"", ""));
						bean.setDcSupportDocType(metaDataObj.get("dc:supportDocType").toString().replaceAll("\"", ""));
						bean.setDcLanguages(metaDataObj.get("dc:languages").toString().replaceAll("\"", ""));
						bean.setDcBrand(metaDataObj.get("dc:brand").toString().replaceAll("\"", ""));
						bean.setDcPersona(metaDataObj.get("dc:persona").toString().replaceAll("\"", ""));
						bean.setDcMediaNumber(Integer.parseInt(metaDataObj.get("dc:mediaNumber").toString().replaceAll("\"", "")));
					}
					preparedStatement = connection.prepareStatement("UPDATE dam.live_link_migration SET type=?, parent=?, name=?, filename=?, sourcesystem=?, root_dam_path=?, extended_path=?, dc_title=?, dc_description=?, dc_category=?, cq_tags=?, dc_category_value=?, dc_support_doc_type=?, dc_languages=?, dc_brand=?, dc_persona=?, dc_media_number=?, meta_data_status=? WHERE livelink_id=?");
					preparedStatement.setString(1, bean.getType());
					preparedStatement.setInt(2, bean.getParent());
					preparedStatement.setString(3, bean.getName());
					preparedStatement.setString(4, bean.getFileName());
					preparedStatement.setString(5, bean.getSourceSystem());
					preparedStatement.setString(6, bean.getRootDAMPath());
					preparedStatement.setString(7, bean.getExtendedPath());
					preparedStatement.setString(8, bean.getDcTitle());
					preparedStatement.setString(9, bean.getDcDescription());
					preparedStatement.setString(10, bean.getDcCategory());
					preparedStatement.setString(11, bean.getCqTags());
					preparedStatement.setString(12, bean.getDcCategoryValue());
					preparedStatement.setString(13, bean.getDcSupportDocType());
					preparedStatement.setString(14, bean.getDcLanguages());
					preparedStatement.setString(15, bean.getDcBrand());
					preparedStatement.setString(16, bean.getDcPersona());
					preparedStatement.setInt(17, bean.getDcMediaNumber());
					preparedStatement.setString(18, "Completed");
					preparedStatement.setInt(19, documentIds.get(i));
					int executeUpdate = preparedStatement.executeUpdate();
					if (executeUpdate == 0)
						LOG.error("Insertion failed in Db for LiveLink ID: " + documentIds.get(i));
					else
						LOG.debug("Insertion successful in DB for LiveLink ID: " + documentIds.get(i));
				} else
					LOG.error("Livelink ID Details API's response is null: " + nodeDetailAPI);
			}
		} catch (SQLException e) {
			LOG.error("SQL Excpetion thrown in insertMetadata method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (DCXDAMException e) {
			LOG.error("DCXDAM Excpetion thrown in insertMetadata method :: AGCOLiveLinkService: {}", e.getMessage());
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close();
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOG.error("Excpetion thrown in insertMetadata method in finally block:: AGCOLiveLinkService: {}", e.getMessage());
			}
		}
	}

	public ArrayList<Integer> uploadFileAssets() {

		ArrayList<Integer> newlyUploadedDocIds = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		PreparedStatement updatePreparedStatement = null;
		ResourceResolver adminResourceResolver = null;
		Connection connection = null;
		InputStream is = null;
		try {
			Map<String, Object> parameter = new HashMap<>();
			parameter.put(ResourceResolverFactory.SUBSERVICE, CoreConstants.DAM_SERVICE_USER);
			adminResourceResolver = resolverFactory.getServiceResourceResolver(parameter);
			AssetManager assetManager = adminResourceResolver.adaptTo(AssetManager.class);
			Session adminSession = adminResourceResolver.adaptTo(Session.class);
			connection = databaseConnection.getConnection();
			preparedStatement = connection.prepareStatement("SELECT livelink_id, type, parent, sourcesystem, filename, root_dam_path, extended_path, dc_title, dc_description, dc_category, cq_tags, dc_category_value, dc_support_doc_type, dc_languages, dc_brand, dc_persona, dc_media_number FROM dam.live_link_migration WHERE meta_data_status= 'Completed' AND file_upload_status = 'Incomplete' ORDER BY livelink_id ASC");
			ResultSet resultSet = preparedStatement.executeQuery();
			String[] cqTags = new String[1];
			String[] dcLanguages = new String[1];
			String[] dcMasterLanguage = new String[1];
			String[] dcMarketAccess = new String[1];
			String[] dcAccessLevels = new String[1];
			String[] dcBrand = new String[1];
			URL Url = null;
			Node jcrContentNode = null;
			Node metadataNode = null;

			while (resultSet.next()) {
				LiveLinkBean bean = new LiveLinkBean();
				bean.setLiveLinkId(resultSet.getInt("livelink_id"));
				bean.setType(resultSet.getString("type"));
				bean.setParent(resultSet.getInt("parent"));
				bean.setSourceSystem(resultSet.getString("sourcesystem"));
				bean.setFileName(resultSet.getString("filename").replaceAll(".pdf", ""));
				bean.setRootDAMPath(resultSet.getString("root_dam_path"));
				bean.setExtendedPath(resultSet.getString("extended_path").replaceAll("[ ()]", "_"));
				bean.setAssetPath(String.format("%s%s", bean.getRootDAMPath(), bean.getExtendedPath()));
				bean.setDcTitle(resultSet.getString("dc_title"));
				bean.setDcDescription(resultSet.getString("dc_description"));
				bean.setDcCategory(resultSet.getString("dc_category"));
				cqTags[0] = resultSet.getString("cq_tags");
				bean.setDcCategoryValue(resultSet.getString("dc_category_value"));
				bean.setDcSupportDocType(resultSet.getString("dc_support_doc_type"));
				dcLanguages[0] = resultSet.getString("dc_languages");
				dcMasterLanguage[0] = resultSet.getString("dc_languages");
				dcMarketAccess[0] = "global";
				dcAccessLevels[0] = "None";
				dcBrand[0] = resultSet.getString("dc_brand");
				bean.setDcPersona(resultSet.getString("dc_persona"));
				bean.setDcMediaNumber(resultSet.getInt("dc_media_number"));
				bean.setAssetDownloadURL(String.format("%s%s", FILE_API, bean.getLiveLinkId()));
				Url = new URL(bean.getAssetDownloadURL());
				URLConnection uCon = Url.openConnection();
				is = uCon.getInputStream();
				bean.setMimeType(uCon.getContentType());
				Boolean status = customAssetUploadService.createAsset(assetManager, bean.getAssetPath() + bean.getFileName() + "." + bean.getMimeType().replaceAll("application/", ""), is, bean.getMimeType(), adminResourceResolver, adminSession);
				if (status == true) {
					jcrContentNode = adminSession.getNode(bean.getAssetPath() + bean.getFileName() + "." + bean.getMimeType().replaceAll("application/", "")).getNode("jcr:content");
					if (jcrContentNode != null) {
						metadataNode = jcrContentNode.getNode("metadata");
						jcrContentNode.setProperty("type", bean.getType());
						jcrContentNode.setProperty("dc:LiveLinkID", bean.getLiveLinkId());
						jcrContentNode.setProperty("parent", bean.getParent());
						jcrContentNode.setProperty("sourceSystem", bean.getSourceSystem());
						if (metadataNode != null) {
							metadataNode.setProperty("dc:description", bean.getDcDescription());
							metadataNode.setProperty("dc:category", bean.getDcCategory());
							metadataNode.setProperty("cq:tags", cqTags);
							metadataNode.setProperty("dc:categoryValue", bean.getDcCategoryValue());
							metadataNode.setProperty("dc:supportDocType", bean.getDcSupportDocType());
							metadataNode.setProperty("dc:languages", dcLanguages);
							metadataNode.setProperty("dc:accessLevels", dcAccessLevels);
							metadataNode.setProperty("dc:brand", dcBrand);
							metadataNode.setProperty("dc:marketAccess", dcMarketAccess);
							metadataNode.setProperty("dc:masterLanguage", dcMasterLanguage);
							metadataNode.setProperty("dc:persona", bean.getDcPersona());
							metadataNode.setProperty("dc:mediaNumber", bean.getDcMediaNumber());
							metadataNode.setProperty("dc:title", bean.getDcTitle());
						}
						adminSession.save();
						updatePreparedStatement = connection.prepareStatement("UPDATE dam.live_link_migration SET file_upload_status=? WHERE livelink_id=?");
						updatePreparedStatement.setString(1, "Completed");
						updatePreparedStatement.setInt(2, bean.getLiveLinkId());
						int statusExecuteUpdate = updatePreparedStatement.executeUpdate();
						if (statusExecuteUpdate == 0)
							LOG.error("Updated File Upload Status as Completed.");
						else
							LOG.debug("Updating File Upload Status as Completed failed.");
						newlyUploadedDocIds.add(bean.getLiveLinkId());
					}
				}
			}
			adminSession.logout();
		} catch (SQLException e) {
			LOG.error("SQL Excpetion thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (DCXDAMException e) {
			LOG.error("DCXDAM Excpetion thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (MalformedURLException e) {
			LOG.error("MalformedURLException thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (IOException e) {
			LOG.error("IOException thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (LoginException e) {
			LOG.error("LoginException thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (PathNotFoundException e) {
			LOG.error("PathNotFoundException thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} catch (RepositoryException e) {
			LOG.error("PathNotFoundException thrown in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
		} finally {
			try {
				if (null != adminResourceResolver && adminResourceResolver.isLive())
					adminResourceResolver.close();
				if (preparedStatement != null)
					preparedStatement.close();
				if (updatePreparedStatement != null)
					updatePreparedStatement.close();
				if (connection != null)
					connection.close();
				try {
					if (is != null) {
						is.close();
					}
				} catch (IOException e) {
					LOG.error("IOException occured in uploadFileAssets method :: AGCOLiveLinkService: {}", e.getMessage());
				}
			} catch (Exception e) {
				LOG.error("Excpetion thrown in uploadFileAssets method in finally block:: AGCOLiveLinkService: {}", e.getMessage());
			}
		}
		return newlyUploadedDocIds;
	}

	public String getJsonResponse(String final_URL) {
		String fullResponse = StringUtils.EMPTY;
		InputStream responseStream = null;
		HttpURLConnection connection = null;
		try {
			URL liveLinkUrl = new URL(final_URL);
			connection = (HttpURLConnection) liveLinkUrl.openConnection();
			// Now it's "open", we can set the request method, headers etc.
			connection.setRequestProperty("accept", "application/json");
			Integer status = connection.getResponseCode();
			if (connection.getResponseCode() != 200)
				LOG.error("RuntimeException thrown.");
			// This line makes the request
			responseStream = connection.getInputStream();
			LOG.debug("responseStream::{}", responseStream);
			try (Reader reader = new InputStreamReader(responseStream, Charsets.UTF_8)) {
				fullResponse = CharStreams.toString(reader);
			}
		} catch (IOException e) {
			LOG.error("Error occured during making call to liveLinkUrl and url is {}", final_URL, e);
		} catch (RuntimeException e) {
			LOG.error("Excpetion thrown in getJsonResponse method:: AGCOLiveLinkService: {}", e.getMessage());
		} finally {
			if (connection != null)
				connection.disconnect();
			if (responseStream != null) {
				try {
					responseStream.close();
				} catch (IOException e) {
					LOG.error("Excpetion thrown in getJsonResponse method in finally block:: AGCOLiveLinkService: {}", e.getMessage());
				}
			}
		}
		return fullResponse;
	}

}

import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import axios from "axios";
import {FarmerData} from "./interfaces";

admin.initializeApp();

const POLYGON_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/farmer_field_layer/FeatureServer/0";
const POINT_LAYER_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/farmer_assessment_layer/FeatureServer/0";

/**
 * Fetches data from the SASA API and returns the output
 * */
const fetchData = async () => {
  const url = "https://api.sasa.solutions/api/eco_bw/farmers";
  const endDate = new Date(); // Current timestamp
  const endTimestamp = Math.floor(endDate.getTime() / 1000) - 24 * 60 * 60;


  // eslint-disable-next-line max-len
  const startDate = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hours ago
  const startTimestamp = Math.floor(startDate.getTime() / 1000) - 24 * 60 * 60;
  const token = "7aSusl5mMW0BCPg8jZI8Fcf5LmF64k0P";

  const headers = {
    Authorization: `${token}`,
  };

  const params = {
    start_at: startTimestamp,
    end_at: endTimestamp,
  };

  try {
    const response = await axios.get(url, {headers, params});
    return response.data;
  } catch (error) {
    functions.logger.error("Error fetching data:", error);
    throw error;
  }
};

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const buildPointFeatureLayer = (farmer: FarmerData) => {
  if (farmer.demographic.home_location) {
    const location = farmer.demographic.home_location;

    const geometry = {
      x: location.longitude,
      y: location.latitude,
      spatialReference: {
        wkid: 4326,
      },
    };

    const attributes = {
      farmer_uuid: farmer.uuid,
      farmer_name: farmer.name,
      farmer_gender: farmer.demographic.gender,
      identity_type: farmer.demographic.identity_type,
      identity_number: farmer.demographic.identity_number,
      farmer_age: farmer.demographic.age,
      created_at: farmer.created_at,
      updated_at: farmer.updated_at,
    };

    return [
      geometry,
      attributes,
    ];
  }
};

const buildPolygonFeatureLayer = (farmer: FarmerData) => {
  const coordinateData = [];
  const featureLayers = [];

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  if (farmer.fields && farmer.fields.length > 0) {
    if (farmer.fields.length > 1) {
      functions.logger.info("Farmer has more than one field");
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // buildPolygonFeatureLayer()
    }


    if (farmer.fields[0]["map"] && farmer.fields[0]["map"].length > 0) {
      for (const coordinates of farmer.fields[0]["map"]) {
        coordinateData.push([coordinates.longitude, coordinates.latitude]);
      }
    }

    // check if the first and last coordinates are the same
    // eslint-disable-next-line max-len
    if (coordinateData[0][0] !== coordinateData[coordinateData.length - 1][0] && coordinateData[0][1] !== coordinateData[coordinateData.length - 1][1]) {
      // eslint-disable-next-line max-len
      // add to the end of the array to close the polygon
      coordinateData.push(coordinateData[0][0], coordinateData[0][1]);
    }
  }

  const geometry = {
    rings: [coordinateData],
    spatialReference: {
      wkid: 4326,
    },
  };

  const attributes = {
    farmer_uuid: farmer.uuid,
    farmer_name: farmer.name,
    farmer_field_uuid: farmer.fields ? farmer.fields[0].uuid : null,
    // eslint-disable-next-line max-len
    farmer_gender: farmer.demographic.gender? farmer.demographic.gender : null,
    // eslint-disable-next-line max-len
    farmer_identity_type: farmer.demographic.identity_type ? farmer.demographic.identity_type : null,
    // eslint-disable-next-line max-len
    farmer_identity_number: farmer.demographic.identity_number ? farmer.demographic.identity_number : null,
    farmer_created_at: farmer.created_at || null,
  };

  const featureLayer = {
    geometry: coordinateData.length > 0 ? geometry : null,
    attributes,
  };

  featureLayers.push(featureLayer);

  return featureLayers;
};

const addPolygonDataToArcGIS = async (data: any) => {
  // eslint-disable-next-line max-len
  const url = `${POLYGON_FEATURE_SERVICE}/addFeatures?f=json`;
  const config = {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
  };

  const formData = {
    "features": JSON.stringify(data),
  };


  try {
    const result = await axios.post(url, formData, config);
    functions.logger.info(result);
    return result;
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
};

const addPointDataToArcGIS = async (data: any) => {
  // eslint-disable-next-line max-len
  const url = `${POINT_LAYER_FEATURE_SERVICE}/addFeatures?f=json`;
  const config = {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
  };

  const formData = {
    "features": JSON.stringify(data),
  };


  try {
    const result = await axios.post(url, formData, config);
    functions.logger.info(result);
    return result;
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
};

const updateDataInArcGIS = async (data: any) => {
  const url = `${POLYGON_FEATURE_SERVICE}/updateFeatures?f=json`;
  const config = {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
  };

  const formData = {
    "features": JSON.stringify(data),
  };

  try {
    const result = await axios.post(url, formData, config);
    functions.logger.info(result);
    return result;
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
};

const deleteDataInArcGIS = async (data: any) => {
  const url = `${POLYGON_FEATURE_SERVICE}/deleteFeatures?f=json`;
  const config = {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
  };

  const formData = {
    "features": JSON.stringify(data),
  };

  try {
    const result = await axios.post(url, formData, config);
    functions.logger.info(result);
    return result;
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
};

const processFarmerData = async ( data: FarmerData[] ) => {
  for (const farmer of data) {
    if (farmer) {
      functions.logger.info(`Processing farmer: ${farmer.uuid} ${farmer.name}`);
      try {
        // check if farmer exists in firestore
        // eslint-disable-next-line max-len
        const farmerRef = await admin.firestore().collection("farmers").doc(farmer.uuid).get();
        if (!farmerRef.exists) {
          const polygonFeature = buildPolygonFeatureLayer(farmer);
          const pointFeature = buildPointFeatureLayer(farmer);

          // eslint-disable-next-line max-len
          const addPolygonResult = await addPolygonDataToArcGIS(polygonFeature);
          const addPointResult = await addPointDataToArcGIS(pointFeature);

          // eslint-disable-next-line max-len
          functions.logger.info( `Successfully added farmer with farmer_uuid ${ farmer.uuid } and farmer_name ${ farmer.name } to ArcGIS` );

          // eslint-disable-next-line max-len
          if (
            addPolygonResult.data.addResults[0].success.toString() == "true" &&
            addPointResult.data.addResults[0].success.toString() == "true"
          ) {
            const {objectId, globalId} = addPolygonResult.data.addResults[0];

            // eslint-disable-next-line max-len
            await admin.firestore().collection( "farmers" ).doc( farmer.uuid ).set( {...farmer, objectId, globalId} );
            // eslint-disable-next-line max-len
            functions.logger.info( `Successfully added farmer with object id ${ objectId } and global id ${ globalId } to Firestore - ${ farmer.uuid }` );
          }
        } else {
          const feature = buildPolygonFeatureLayer( farmer );
          // update farmer in firestore
          const result = await updateDataInArcGIS( feature );

          if (result.data.updateResults[0].success.toString() == "true") {
            // eslint-disable-next-line max-len
            await admin.firestore().collection( "farmers" ).doc( farmer.uuid ).update( {...farmer} );
            // eslint-disable-next-line max-len
            functions.logger.info(`Successfully updated farmer with farmer_uuid ${ farmer.uuid } and farmer_name ${ farmer.name } to ArcGIS` );
          }
        }
      } catch (error) {
        functions.logger.error(error);
        throw (error);
      }
    }
  }
};

// const deleteData = async (data: FarmerData) => {
//   const featureLayer = buildPolygonFeatureLayer(data);
//
//   try {
//     // using Promise.all to run both functions at the same time
//     await Promise.all( [
//       // eslint-disable-next-line max-len
//       admin.firestore().collection( "farmers" ).doc(data.uuid).delete(),
//       deleteDataInArcGIS( featureLayer ),
//     ] );
//     // eslint-disable-next-line max-len
// eslint-disable-next-line max-len
//     functions.logger.info(`Successfully deleted farmer with farmer_uuid ${data.uuid} and farmer_name ${data.name} from ArcGIS`);
//   } catch (error) {
//     functions.logger.error("Error deleting farmer data");
//     throw (error);
//   }
// };

// const updateData = async (data: FarmerData) => {
//   const featureLayer = buildPolygonFeatureLayer(data);
//
//   try {
//     // using Promise.all to run both functions at the same time
//     await Promise.all( [
//       // eslint-disable-next-line max-len,@typescript-eslint/ban-ts-comment
//       // @ts-ignore
//       // eslint-disable-next-line max-len
// eslint-disable-next-line max-len
//       admin.firestore().collection( "farmers" ).doc(data.uuid).update( {...data}),
//       updateDataInArcGIS( featureLayer ),
//     ] );
//     // eslint-disable-next-line max-len
// eslint-disable-next-line max-len
//     functions.logger.info(`Successfully updated farmer with farmer_uuid ${data.uuid} and farmer_name ${data.name} from ArcGIS`);
//   } catch (error) {
//     functions.logger.error("Error updating farmer data");
//     throw (error);
//   }
// };

// const addData = async (data: FarmerData) => {
//   const featureLayer = buildPolygonFeatureLayer(data);
//
//   try {
//     // using Promise.all to run both functions at the same time
//     await Promise.all( [
//       // eslint-disable-next-line max-len,@typescript-eslint/ban-ts-comment
//       // @ts-ignore
//       admin.firestore().collection( "farmers" ).doc(data.uuid).set(data),
//       addPolygonDataToArcGIS( featureLayer ),
//     ] );
//     // eslint-disable-next-line max-len
// eslint-disable-next-line max-len
//     functions.logger.info(`Successfully added farmer with farmer_uuid ${data.uuid} and farmer_name ${data.name} to ArcGIS`);
//   } catch (error) {
//     functions.logger.error("Error adding farmer data");
//     throw (error);
//   }
// };

// main function that will be triggered every day at 04:50 UTC
// eslint-disable-next-line max-len
exports.dailyScheduledFunction = functions.pubsub.schedule("50 4 * * *").onRun(async ( context ) => {
  // info log the function time start in UTC
  functions.logger.info( "This will be run every day at 04:50 UTC!" );
  try {
    const data: FarmerData[] = await fetchData();
    await processFarmerData( data );
  } catch (error) {
    functions.logger.error( error );
  }
});

// eslint-disable-next-line max-len
exports.manualAddData = functions.https.onRequest(async ( request, response ) => {
  try {
    const farmData: FarmerData = request.body.data[0];

    const feature = buildPolygonFeatureLayer( farmData );
    functions.logger.log(JSON.stringify(feature));
    const result = await addPolygonDataToArcGIS( feature );

    if (result.data.addResults[0].success.toString() == "true") {
      const {objectId, globalId} = result.data.addResults[0];
      // eslint-disable-next-line max-len
      functions.logger.info(`Successfully added farmer with objectId: ${objectId} and globalId: ${globalId} to ArcGIS`);

      // eslint-disable-next-line max-len
      await admin.firestore().collection("farmers").doc(farmData.uuid).set({...farmData, objectId, globalId});
      response.status(200).send(JSON.stringify(result.data.addResults));
    }
  } catch (error) {
    functions.logger.error(error);
    response.status(500).send(error);
  }
});

// eslint-disable-next-line max-len
exports.manualDeleteData = functions.https.onRequest(async ( request, response ) => {
  const {farmerUuid} = request.body;

  try {
    // fetch farmer data from firestore
    // eslint-disable-next-line max-len
    const farmer = await admin.firestore().collection("farmers").doc(farmerUuid).get();
    if (farmer.exists) {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      const farmerData = farmer.data() as FarmerData;
      const feature = buildPolygonFeatureLayer(farmerData);
      const result = await deleteDataInArcGIS( feature );

      if (result.data.deleteResults[0].success.toString() == "true") {
        // eslint-disable-next-line max-len
        functions.logger.info(`Successfully deleted farmer with objectId: ${farmerData.objectId} and globalId: ${farmerData.globalId} from ArcGIS`);

        // eslint-disable-next-line max-len
        await admin.firestore().collection("farmers").doc(farmerUuid).delete();
        response.status(200).send(JSON.stringify(result.data.deleteResults));
      }
    }
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
});


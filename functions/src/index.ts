import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import axios from "axios";
import {FarmerData} from "./interfaces";

admin.initializeApp();

const FEATURE_SERVICE_URL = "https://services8.arcgis.com/MEMDKjzGcWOqQfau/arcgis/rest/services/sasa_integration/FeatureServer";

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

const buildFeatureLayer = (farmer: FarmerData) => {
  const coordinateData = [];
  console.log(farmer.feilds);
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  if (farmer.feilds && farmer.feilds.length > 0) {
    if (farmer.feilds[0]["map"] && farmer.feilds[0]["map"].length > 0) {
      for (const coordinates of farmer.feilds[0]["map"]) {
        coordinateData.push([coordinates.longitude, coordinates.latitude]);
      }
    }

    // check if the first and last coordinates are the same
    // eslint-disable-next-line max-len
    if (coordinateData[0][0] !== coordinateData[coordinateData.length - 1][0] && coordinateData[0][1] !== coordinateData[coordinateData.length - 1][1]) {
      // eslint-disable-next-line max-len
      coordinateData.push([farmer.feilds[0].location?.longitude, farmer.feilds[0].location?.latitude]);
    }
  }

  const geometry = {
    rings: [coordinateData],
    spatialReference: {
      wkid: 4326,
    },
  };

  return [
    {
      geometry: coordinateData.length > 0 ? geometry : null,
      attributes: {
        farmer_uuid: farmer.uuid,
        farmer_name: farmer.name,
        farmer_field_uuid: farmer.uuid || null,
        farmer_gender: farmer.demographic.gender || null,
        farmer_identity_type: farmer.demographic.identity_type || null,
        farmer_identity_number: farmer.demographic.identity_number || null,
        farmer_created_at: farmer.created_at || null,
      },
    },
  ];
};

const addDataToArcGIS = async (data: any) => {
  // eslint-disable-next-line max-len
  const url = `${FEATURE_SERVICE_URL}/0/addFeatures?f=json`;
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
  const url = `${FEATURE_SERVICE_URL}/0/updateFeatures?f=json`;
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
    const result = await axios.put(url, formData, config);
    functions.logger.info(result);
    return result;
  } catch (error) {
    functions.logger.error(error);
    throw (error);
  }
};

const deleteDataInArcGIS = async (data: any) => {
  const url = `${FEATURE_SERVICE_URL}/0/deleteFeatures?f=json`;
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
    const result = await axios.put(url, formData, config);
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
        const feature = buildFeatureLayer( farmer );
        const result = await addDataToArcGIS( feature );
        // eslint-disable-next-line max-len
        functions.logger.info(`Successfully added farmer with farmer_uuid ${farmer.uuid} and farmer_name ${farmer.name} to ArcGIS`);

        if (result.data.addResults[0].success.toString() == "true") {
          const {objectId, globalId} = result.data.addResults[0];

          // eslint-disable-next-line max-len
          await admin.firestore().collection("farmers").doc(farmer.uuid).set({...farmer, objectId, globalId});
          // eslint-disable-next-line max-len
          functions.logger.info(`Successfully added farmer with object id ${objectId} and global id ${globalId} to Firestore - ${farmer.uuid}`);
        }
      } catch (error) {
        functions.logger.error(error);
        throw (error);
      }
    }
  }
};


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
exports.manualTestFunction = functions.https.onRequest(async ( request, response ) => {
  try {
    const farmData: FarmerData = request.body.data[0];

    // TODO work on this tomorrow
    // TODO build function to edit feature layer
    // TODO build function to delete feature layer
    // check if the farmer data already exists in the database
    // eslint-disable-next-line max-len
    // const farmer = await admin.firestore().collection("farmers").doc(farmData.uuid).get();
    // if (farmer.exists) {
    //   // check if farmer data has changed
    //   if (farmer.data() !== farmData) {
    //
    //   }
    // }

    const feature = buildFeatureLayer( farmData );
    functions.logger.log(JSON.stringify(feature));
    const result = await addDataToArcGIS( feature );

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

import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import axios from "axios";
import {FarmerData, FeatureLayer, Location} from "./interfaces";

admin.initializeApp();

const POLYGON_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/Farmer_Field_Data_Layer/FeatureServer/0";
const POINT_LAYER_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/Farmer_Demographic_Layer/FeatureServer/0";

const fetchData = async () => {
  const url = "https://api.sasa.solutions/api/eco_bw/farmers";
  const endDate = new Date(); // Current timestamp
  const endTimestamp = endDate.getTime() / 1000;

  const startDate = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hours ago
  const startTimestamp = Math.floor(startDate.getTime() / 1000);
  const token = "7aSusl5mMW0BCPg8jZI8Fcf5LmF64k0P";


  functions.logger.info("These are the timestamps:", {startTimestamp, endTimestamp});

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
  if (farmer.demographic.location) {
    const location = farmer.demographic.location;

    const geometry = {
      x: location.longitude,
      y: location.latitude,
      spatialReference: {
        wkid: 4326,
      },
    };

    const attributes = {
      farmer_uuid: farmer.uuid ? farmer.uuid : null,
      farmer_name: farmer.name ? farmer.name : null,
      farmer_gender: farmer.demographic.gender,
      identity_type: farmer.demographic.identity_type,
      identity_number: farmer.demographic.identity_number,
      farmer_age: farmer.demographic.age,
    };

    return {
      geometry,
      attributes,
    };
  }
};

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const buildPolygonFeatureLayer = (farmer: FarmerData): {
  geometry: { rings: [ Location[] ][] | null; spatialReference: { wkid: number } } | null;
  attributes: {
    farmer_created_at: string | null;
    farmer_gender: string | null;
    farmer_identity_type: string | null;
    farmer_field_uuid: string | null;
    farmer_uuid: string | null | undefined;
    farmer_identity_number: string | null;
    farmer_name: string | null | undefined
  }
} => {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const coordinateData: [Location[]] = [];
  const featureLayers: [FeatureLayer] | any = [];

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  if (farmer) {
    if (farmer.fields) {
      if (farmer.fields.length > 1) {
        functions.logger.info("Farmer has more than one field");

        for (const field of farmer.fields) {
          const newFarmer = farmer;
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          const fieldCoordinateData: [Location[]] = [];

          newFarmer.fields = [field];
          functions.logger.info("newFarmer", newFarmer);
          functions.logger.info("newFarmer.fields", newFarmer.fields);
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          // build polygon layer here

          if (Array.isArray(farmer.fields)) {
            const field = farmer.fields[0];

            if (field["map"] && field["map"].length > 0) {
              for (const coordinates of field["map"]) {
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-ignore
                fieldCoordinateData.push([coordinates.longitude, coordinates.latitude]);
              }

              if (fieldCoordinateData && fieldCoordinateData.length > 0) {
                if (fieldCoordinateData[0][0] !== fieldCoordinateData[fieldCoordinateData.length - 1][0] && fieldCoordinateData[0][1] !== fieldCoordinateData[fieldCoordinateData.length - 1][1]) {
                  // add to the end of the array to close the polygon
                  fieldCoordinateData.push([fieldCoordinateData[0][0], fieldCoordinateData[0][1]]);
                }
              }
            }
          }

          const geometry = {
            rings: [fieldCoordinateData],
            spatialReference: {
              wkid: 4326,
            },
          };

          const attributes = {
            farmer_uuid: newFarmer.uuid,
            farmer_name: newFarmer.name,
            farmer_field_uuid: newFarmer.fields && newFarmer.fields[0] ? newFarmer.fields[0].uuid : null,
            farmer_gender: newFarmer.demographic.gender? newFarmer.demographic.gender : null,
            farmer_identity_type: newFarmer.demographic.identity_type ? newFarmer.demographic.identity_type : null,
            farmer_identity_number: newFarmer.demographic.identity_number ? newFarmer.demographic.identity_number : null,
            farmer_created_at: newFarmer.created_at || null,
          };

          const polygonLayer = {
            geometry: fieldCoordinateData.length > 1 ? geometry : null,
            attributes,
          };

          featureLayers.push(polygonLayer);
        }

        functions.logger.info("featureLayers", featureLayers);

        return featureLayers;
      }
    }

    functions.logger.info("Farmer has one field");
    functions.logger.info("farmer", farmer);

    if (farmer.fields) {
      if (Array.isArray(farmer.fields)) {
        functions.logger.info("farmer.fields is an array: ", farmer.fields);
        const field = farmer.fields[0];
        if (field["map"] && field["map"].length > 0) {
          for (const coordinates of field["map"]) {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            coordinateData.push([coordinates.longitude, coordinates.latitude]);
          }
        }
      }
    }

    functions.logger.info("coordinateData", coordinateData);

    if (coordinateData && coordinateData.length > 0) {
      if (coordinateData[0][0] !== coordinateData[coordinateData.length - 1][0] && coordinateData[0][1] !== coordinateData[coordinateData.length - 1][1]) {
        // add to the end of the array to close the polygon
        coordinateData.push([coordinateData[0][0], coordinateData[0][1]]);
      }
    }

    const geometry = {
      rings: [coordinateData] || null,
      spatialReference: {
        wkid: 4326,
      },
    };

    const attributes = {
      farmer_uuid: farmer.uuid,
      farmer_name: farmer.name,
      farmer_field_uuid: farmer.fields && farmer.fields[0] ? farmer.fields[0].uuid : null,
      farmer_gender: farmer.demographic.gender? farmer.demographic.gender : null,
      farmer_identity_type: farmer.demographic.identity_type ? farmer.demographic.identity_type : null,
      farmer_identity_number: farmer.demographic.identity_number ? farmer.demographic.identity_number : null,
      farmer_created_at: farmer.created_at || null,
    };

    functions.logger.info("geometry", geometry);
    functions.logger.info("attributes", attributes);

    return {
      geometry: coordinateData.length > 1 ? geometry : null,
      attributes,
    };
  }
};

const addPolygonDataToArcGIS = async (data: any) => {
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

const saveAllDataToFirebase = async (data: FarmerData[]) => {
  for (const farmer of data) {
    await admin.database().ref(`sasa-raw-data/sasa-data-list/${farmer.uuid}`).set({
      ...farmer,
      polygonFeatureDataSynced: false,
      pointFeatureDataSynced: false,
      lastPolygonDataSyncEvent: Date.now(),
      lastPointDataSyncEvent: Date.now(),
    });
  }
};

// synchronise data on a daily basis

exports.dailySasaDataSync = functions.pubsub.schedule("50 4 * * *").onRun(async ( context ) => {
  try {
    const data: FarmerData[] = await fetchData();
    await saveAllDataToFirebase(data);
  } catch (error) {
    functions.logger.error( error );
  }
});

// Generate polygon feature layers

exports.generatePolygonFeatureLayers = functions.pubsub.schedule("every 30 minutes").onRun(async ( context ) => {
  try {
    // retrieve 5 records from firebase db where dataSynced is false
    const sasaDataListRef = admin.database().ref("/sasa-raw-data");
    const snapshot = await sasaDataListRef.child("sasa-data-list").orderByChild("polygonFeatureDataSynced").equalTo(false).limitToFirst(15).once( "value");
    const availableData = snapshot.val();
    if (!availableData) {
      functions.logger.info( "No data to process" );
      return;
    }

    functions.logger.info( "Available data to process", availableData);

    // iterate through availableData
    // eslint-disable-next-line guard-for-in
    for (const key in availableData) {
      // eslint-disable-next-line no-prototype-builtins
      if (availableData.hasOwnProperty(key)) {
        const farmer = availableData[key];
        // build polygon feature layer
        const polygonFeature = buildPolygonFeatureLayer(farmer);
        if (polygonFeature) {
          if (Array.isArray(polygonFeature)) {
            for (const feature of polygonFeature) {
              // this will need to change slightly because there will only be one record with this uuid (but this should be multiple)

              await admin.database().ref(`polygon-feature-layers/polygon-layers-list/${feature.attributes.farmer_field_uuid}`).set({
                ...feature,
                featureLayerCreated: false,
                lastUpdated: Date.now(),
              });
            }
          } else {
            await admin.database().ref(`polygon-feature-layers/polygon-layers-list/${farmer.uuid}`).set({
              ...polygonFeature,
              featureLayerCreated: false,
              lastUpdated: Date.now(),
            });
          }

          await admin.database().ref("sasa-raw-data/sasa-data-list").child(farmer.uuid).update({
            polygonFeatureDataSynced: true,
            lastPolygonDataSyncEvent: Date.now(),
          });

          functions.logger.info( "polygon feature layer saved", polygonFeature );
          functions.logger.info( `farmer ${ farmer.uuid } dataSynced set to true` );
        }
      }
    }
  } catch (error) {
    functions.logger.error(error);
  }
});

exports.sendPolygonFeatureLayersToArcGIS = functions.pubsub.schedule("every 30 minutes").onRun(async ( context ) => {
  const polygonFeatureRef = admin.database().ref("/polygon-feature-layers");
  const snapshot = await polygonFeatureRef.child("polygon-layers-list").orderByChild("featureLayerCreated").equalTo(false).limitToFirst(5).once( "value");
  const availableData = snapshot.val();

  const features = [];

  if (!availableData) {
    functions.logger.info( "No data to process" );
    return;
  }

  for (const key in availableData) {
    // eslint-disable-next-line no-prototype-builtins
    if (availableData.hasOwnProperty(key)) {
      const polygonFeature = availableData[key];
      // build polygon feature layer
      functions.logger.info("polygonFeature", polygonFeature);
      // this is a single feature layer
      if (!polygonFeature.geometry) {
        functions.logger.info("No geometry to process");
        await admin.database().ref("polygon-feature-layers/polygon-layers-list").child(key).update({
          featureLayerCreated: true,
          isGeometryEmpty: true,
          reasonFailure: "No geometry to process",
          lastUpdated: Date.now(),
        });
        continue;
      }

      const featureLayer = {
        geometry: polygonFeature.geometry,
        attributes: polygonFeature.attributes,
      };

      features.push(featureLayer);

      await admin.database().ref("polygon-feature-layers/polygon-layers-list").child(key).update({
        featureLayerCreated: true,
        lastUpdated: Date.now(),
      });
    }
  }

  if (features.length < 1) {
    functions.logger.info("No features to process");
    return;
  }

  const result = await addPolygonDataToArcGIS(features);
  functions.logger.info("addPolygonDataToArcGIS result: ", result);
});

// Generate point feature layers

exports.generatePointFeatureLayers = functions.pubsub.schedule("every 30 minutes").onRun(async ( context ) => {
  try {
    // retrieve 5 records from firebase db where dataSynced is false
    const sasaDataListRef = admin.database().ref("/sasa-raw-data");
    const snapshot = await sasaDataListRef.child("sasa-data-list").orderByChild("pointFeatureDataSynced").equalTo(false).limitToFirst(15).once( "value");
    const availableData = snapshot.val();

    if (!availableData) {
      functions.logger.info( "No data to process" );
      return;
    }

    for (const key in availableData) {
      // eslint-disable-next-line no-prototype-builtins
      if (availableData.hasOwnProperty(key)) {
        const farmer = availableData[key];
        // build point feature layer
        const pointFeature = buildPointFeatureLayer(farmer);
        if (pointFeature) {
          await admin.database().ref(`point-feature-layers/point-layers-list/${farmer.uuid}`).set({
            ...pointFeature,
            featureLayerCreated: false,
            lastUpdated: Date.now(),
          });

          await admin.database().ref("sasa-raw-data/sasa-data-list").child(farmer.uuid).update({
            pointFeatureDataSynced: true,
            lastPointDataSyncEvent: Date.now(),
          });

          functions.logger.info( "point feature layer saved", pointFeature );
          functions.logger.info( `farmer ${ farmer.uuid } dataSynced set to true` );
        }
      }
    }

    functions.logger.info( "Available data to process", availableData);
  } catch (error) {
    functions.logger.error(error);
  }
});

exports.sendPointFeatureLayersToArcGIS = functions.pubsub.schedule("every 30 minutes").onRun(async ( context ) => {
  try {
    const polygonFeatureRef = admin.database().ref("/point-feature-layers");
    const snapshot = await polygonFeatureRef.child("point-layers-list").orderByChild("featureLayerCreated").equalTo(false).limitToFirst(5).once( "value");
    const availableData = snapshot.val();

    if (!availableData) {
      functions.logger.info( "No data to process" );
      return;
    }

    for (const key in availableData) {
      // eslint-disable-next-line no-prototype-builtins
      if (availableData.hasOwnProperty(key)) {
        const pointFeature = availableData[key];
        // build polygon feature layer
        functions.logger.info("pointFeature", pointFeature);
        // this is a single feature layer
        if (!pointFeature.geometry) {
          functions.logger.info("No point data to process");
          await admin.database().ref("point-feature-layers/point-layers-list").child(key).update({
            featureLayerCreated: true,
            isLocationEmpty: true,
            reasonFailure: "No location to process",
            lastUpdated: Date.now(),
          });
          continue;
        }

        const featureLayer = {
          geometry: pointFeature.geometry,
          attributes: pointFeature.attributes,
        };
        const result = await addPointDataToArcGIS([featureLayer]);
        const {objectId} = result.data.addResults[0];

        await admin.database().ref("point-feature-layers/point-layers-list").child(key).update({
          objectId,
          featureLayerCreated: true,
          lastUpdated: Date.now(),
        });
      }
    }
  } catch (error) {
    functions.logger.error(error);
  }
});

exports.manualSendPolygonFeatureLayersToArcGIS = functions.https.onRequest(async ( req, res ) => {
  const uuid = req.query.uuid;
  if (uuid && typeof uuid === "string") {
    const polygonFeatureRef = admin.database().ref("/polygon-feature-layers");
    const snapshot = await polygonFeatureRef.child("polygon-layers-list").child(uuid).once( "value");
    const availableData = snapshot.val();

    const features = [];

    if (!availableData) {
      functions.logger.info( "No data to process" );
      res.send("No data to process");
    }

    const polygonFeature = {
      attributes: availableData.attributes,
      geometry: availableData.geometry,
    };

    features.push(polygonFeature);

    functions.logger.info(polygonFeature);
    functions.logger.info("features", features);

    // send data to arcgis
    const result = await addPolygonDataToArcGIS(features);

    await admin.database().ref("polygon-feature-layers/polygon-layers-list").child(uuid).update({
      featureLayerCreated: true,
      lastUpdated: Date.now(),
    });
    functions.logger.info("addPolygonDataToArcGIS result: ", result.data);
  }

  res.send("No uuid provided");
});

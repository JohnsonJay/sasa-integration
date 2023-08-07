import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import axios from "axios";
import {FarmerData, FeatureLayer, Location} from "./interfaces";

admin.initializeApp();

// const POLYGON_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/Farm_Land_Layer/FeatureServer/0";
// const POINT_LAYER_FEATURE_SERVICE = "https://services-eu1.arcgis.com/gq4tFiP3X79azbdV/arcgis/rest/services/farmer_assessment_layer/FeatureServer/0";

/**
 * Fetches data from the SASA API and returns the output
 * */
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

// const writeDataToFirebaseStorage = (data: any) => {
//   // write data to firebase storage
//   const bucket = admin.storage().bucket();
//   const file = bucket.file("farmers.json");
//
//   // const stringifiedData = JSON.stringify(data);
//   const buffer = Buffer.from(data, "utf8");
//
//   file.save(buffer, {
//     gzip: true,
//     metadata: {
//       cacheControl: "public, max-age=31536000",
//       contentType: "application/json",
//     },
//   }).then(() => {
//     functions.logger.info("File written successfully");
//   }).catch((error) => {
//     functions.logger.error("Error writing file:", error);
//   });
// };

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
      farmer_uuid: farmer.uuid ? farmer.uuid : null,
      farmer_name: farmer.name ? farmer.name : null,
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

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const buildPolygonFeatureLayer = (farmer: FarmerData): FeatureLayer | FeatureLayer[] => {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const coordinateData: [Location[]] = [];
  const featureLayers: [FeatureLayer] | any = [];

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  if (farmer.fields && farmer.fields.length > 0) {
    if (farmer.fields.length > 1) {
      functions.logger.info("Farmer has more than one field");
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // buildPolygonFeatureLayer()


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

            if (fieldCoordinateData) {
              if (fieldCoordinateData[0][0] !== fieldCoordinateData[fieldCoordinateData.length - 1][0] && fieldCoordinateData[0][1] !== fieldCoordinateData[fieldCoordinateData.length - 1][1]) {
                // add to the end of the array to close the polygon
                fieldCoordinateData.push([fieldCoordinateData[0][0], fieldCoordinateData[0][1]]);
              }
            }
          }
        }

        const geometry = {
          rings: fieldCoordinateData,
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
          geometry: fieldCoordinateData.length > 0 ? geometry : null,
          attributes,
        };

        featureLayers.push(polygonLayer);
      }

      functions.logger.info("featureLayers", featureLayers);

      return featureLayers;
    }

    functions.logger.info("farmer", farmer);

    if (Array.isArray(farmer.fields)) {
      const field = farmer.fields[0];
      if (field["map"] && field["map"].length > 0) {
        for (const coordinates of field["map"]) {
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          coordinateData.push([coordinates.longitude, coordinates.latitude]);
        }
      }
    }

    functions.logger.info("coordinateData", coordinateData);

    // check if the first and last coordinates are the same

    if (coordinateData) {
      if (coordinateData[0][0] !== coordinateData[coordinateData.length - 1][0] && coordinateData[0][1] !== coordinateData[coordinateData.length - 1][1]) {
        // add to the end of the array to close the polygon
        coordinateData.push([coordinateData[0][0], coordinateData[0][1]]);
      }
    }

    functions.logger.info("coordinateData", coordinateData);
  }

  const geometry = {
    rings: coordinateData,
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

  return {
    geometry: coordinateData.length > 0 ? geometry : null,
    attributes,
  };
};

// eslint-disable-next-line @typescript-eslint/no-unused-vars
// const addPolygonDataToArcGIS = async (data: any) => {
//
//   const url = `${POLYGON_FEATURE_SERVICE}/addFeatures?f=json`;
//   const config = {
//     headers: {
//       "Content-Type": "application/x-www-form-urlencoded",
//       "Accept": "application/json",
//     },
//   };
//
//   const formData = {
//     "features": JSON.stringify(data),
//   };
//
//
//   try {
//     const result = await axios.post(url, formData, config);
//     functions.logger.info(result);
//     return result;
//   } catch (error) {
//     functions.logger.error(error);
//     throw (error);
//   }
// };
//
// const addPointDataToArcGIS = async (data: any) => {
//
//   const url = `${POINT_LAYER_FEATURE_SERVICE}/addFeatures?f=json`;
//   const config = {
//     headers: {
//       "Content-Type": "application/x-www-form-urlencoded",
//       "Accept": "application/json",
//     },
//   };
//
//   const formData = {
//     "features": JSON.stringify(data),
//   };
//
//
//   try {
//     const result = await axios.post(url, formData, config);
//     functions.logger.info(result);
//     return result;
//   } catch (error) {
//     functions.logger.error(error);
//     throw (error);
//   }
// };
//
// const updateDataInArcGIS = async (data: any) => {
//   const url = `${POLYGON_FEATURE_SERVICE}/updateFeatures?f=json`;
//   const config = {
//     headers: {
//       "Content-Type": "application/x-www-form-urlencoded",
//       "Accept": "application/json",
//     },
//   };
//
//   const formData = {
//     "features": JSON.stringify(data),
//   };
//
//   try {
//     const result = await axios.post(url, formData, config);
//     functions.logger.info(result);
//     return result;
//   } catch (error) {
//     functions.logger.error(error);
//     throw (error);
//   }
// };

// const processFarmerData = async ( data: FarmerData[] ) => {
//   for (const farmer of data) {
//     if (farmer) {
//       functions.logger.info(`Processing farmer: ${farmer.uuid} ${farmer.name}`);
//       try {
//         if (farmer.uuid) {
//           functions.logger.info(`Processing farmer data: ${JSON.stringify(farmer)}`);
//
//           const farmerRef = await admin.firestore().collection("farmers").doc(farmer.uuid).get();
//           if (!farmerRef.exists) {
//             await saveFeatureLayerDataToFirestoreAndFirebase(farmer);
//
//             // const addPolygonResult = await addPolygonDataToArcGIS(polygonFeature);
//             // const addPointResult = await addPointDataToArcGIS(pointFeature);
//
//             // functions.logger.info("addPolygonResult", addPolygonResult);
//             // functions.logger.info("addPointResult", addPointResult);
//
//
//             // if (
//             //
//
//             //   addPolygonResult.data.addResults[0].success.toString() == "true" ||
//
//             //     addPointResult.data.addResults[0].success.toString() == "true"
//             // ) {
//             //
//             //   functions.logger.info( `Successfully added farmer with farmer_uuid ${ farmer.uuid } and farmer_name ${ farmer.name } to ArcGIS` );
//             //   // const {objectId, globalId} = addPolygonResult.data.addResults[0];
//             //
//             //
//             //
//             //   functions.logger.info( `Successfully added farmer with object id ${ objectId } and global id ${ globalId } to Firestore - ${ farmer.uuid }` );
//             // }
//           } else {
//             await saveFeatureLayerDataToFirestoreAndFirebase(farmer);
//             // const feature = buildPolygonFeatureLayer( farmer );
//             // functions.logger.info(
//             //   "this is the feature: ",
//             //   JSON.stringify(feature)
//             // );
//             // // update farmer in firestore
//             // const result = await updateDataInArcGIS( feature );
//             // functions.logger.info("this is the result: ", result);
//             //
//             // if (result.data.error) {
//             //   throw new Error(result.data.error.details[0]);
//             // }
//             //
//             // if (
//             //   result.data.updateResults &&
//             //     result.data.updateResults[0].success.toString() == "true"
//             // ) {
//             //
//             //   if ( farmer.uuid != null ) {
//             //
//             //     await admin.firestore().collection( "farmers" ).doc( farmer.uuid ).update( {...farmer} );
//             //   }
//             //
//             //   functions.logger.info(`Successfully updated farmer with farmer_uuid ${ farmer.uuid } and farmer_name ${ farmer.name } to ArcGIS` );
//             // }
//           }
//         }
//       } catch (error) {
//         functions.logger.error(error);
//         throw (error);
//       }
//     }
//   }
// };

// const saveFeatureLayerDataToFirestoreAndFirebase = async (farmer: any) => {
//   const polygonFeature = buildPolygonFeatureLayer(farmer);
//   const pointFeature = buildPointFeatureLayer(farmer);
//
//   functions.logger.info("polygonFeature", polygonFeature);
//   functions.logger.info("pointFeature", pointFeature);
//
//   await Promise.all([
//     admin.firestore().collection( "farmers" ).doc( farmer.uuid ).set( {
//       ...farmer,
//       // objectId,
//       // globalId,
//     }),
//     admin.database().ref(`"polygon-feature-layers"/${farmer.uuid}`).set({
//       ...polygonFeature,
//       lastUpdated: Date.now(),
//     }),
//     admin.database().ref(`"point-feature-layers"/${farmer.uuid}`).set({
//       ...pointFeature,
//       lastUpdated: Date.now(),
//     }),
//   ]);
// };

const saveAllDataToFirebase = async (data: FarmerData[]) => {
  for (const farmer of data) {
    await admin.database().ref(`sasa-raw-data/sasa-data-list/${farmer.uuid}`).set({
      ...farmer,
      dataSynced: false,
      lastDataSyncEvent: Date.now(),
    });
  }
};

// main function that will be triggered every day at 04:50 UTC

// exports.dailyScheduledFunction = functions.pubsub.schedule("50 4 * * *").onRun(async ( context ) => {
//   // info log the function time start in UTC
//   functions.logger.info( "This will be run every day at 04:50 UTC!" );
//   try {
//     const data: FarmerData[] = await fetchData();
//     await Promise.allSettled([
//       processFarmerData(data),
//       saveAllDataToFirebase(data),
//     ]);
//     // await writeDataToFirebaseStorage( data );
//     // await processFarmerData( data );
//   } catch (error) {
//     functions.logger.error( error );
//   }
// });

exports.dailySasaDataSync = functions.pubsub.schedule("50 4 * * *").onRun(async ( context ) => {
  try {
    const data: FarmerData[] = await fetchData();
    await saveAllDataToFirebase(data);
  } catch (error) {
    functions.logger.error( error );
  }
});

exports.generateFeatureLayers = functions.pubsub.schedule("every 30 minutes").onRun(async ( context ) => {
  try {
    // retrieve 5 records from firebase db where dataSynced is false
    const sasaDataListRef = admin.database().ref("/sasa-raw-data");
    await sasaDataListRef.child("sasa-data-list").orderByChild("dataSynced").equalTo(false).limitToFirst(5).once( "value", async ( snapshot ) => {
      const availableData = snapshot.val();
      if (!availableData) {
        functions.logger.info( "No data to process" );
        return;
      }

      // iterate through availableData
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

                await admin.database().ref(`polygon-feature-layers/polygon-layers-list/${farmer.uuid}/${feature.attributes.farmer_field_uuid}`).set({
                  ...feature,
                  dataSynced: false,
                  lastUpdated: Date.now(),
                });
              }
            } else {
              await admin.database().ref(`polygon-feature-layers/polygon-layers-list/${farmer.uuid}`).set({
                ...polygonFeature,
                dataSynced: false,
                lastUpdated: Date.now(),
              });
            }

            await admin.database().ref("sasa-raw-data/sasa-data-list").child(farmer.uuid).update({
              dataSynced: true,
              lastDataSyncEvent: Date.now(),
            });

            functions.logger.info( "polygon feature layer saved", polygonFeature );
            functions.logger.info( `farmer ${ farmer.uuid } dataSynced set to true` );
          }
        }
      }

      functions.logger.info( "Available data to process", availableData);
    }, ( error ) => {
      functions.logger.error( error );
    });
  } catch (error) {
    functions.logger.error(error);
  }
});


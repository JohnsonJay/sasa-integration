export interface MapPoint {
    accuracy: number;
    latitude: number;
    longitude: number;
}

export interface Location {
    latitude: number;
    longitude: number;
    accuracy?: number;
}

export interface Fields {
    uuid: string;
    name: string;
    area: number;
    area_unit: string;
    created_at: string;
    updated_at: string;
    location?: Location;
    map?: MapPoint[];
}

export interface FarmerData {
    uuid?: string | null;
    name?: string | null;
    created_at: string;
    updated_at: string;
    msisdn?: string;
    demographic: {
        gender?: string;
        identity_type?: string;
        identity_number?: string;
        location?: Location;
        work_location?: Location;
        home_location?: Location;
        age?: number;
    }
    fields?: Fields[];
    objectId?: number;
    globalId?: string;
}

export interface FeatureLayer {
    geometry?: {
        rings: [[Location[]]],
        spatialReference: {
            wkid: number,
        },
    } | null;
    attributes: {
        objectId?: number | null;
        farmer_uuid?: string | null;
        farmer_name?: string | null;
        farmer_field_uuid?: string | null;
        farmer_gender?: string | null;
        farmer_identity_type?: string | null;
        farmer_identity_number?: string | null;
        farmer_created_at?: string | null;
    };
}

export interface FeatureLayerResponse {
    globalId: string | null;
    objectId: number;
    success: boolean;
    uniqueId: number;
}

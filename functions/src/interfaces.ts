export interface MapPoint {
    accuracy: number;
    latitude: number;
    longitude: number;
}

export interface Location {
    latitude: number;
    longitude: number;
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
    uuid: string;
    name: string;
    created_at: string;
    updated_at: string;
    msisdn?: string;
    demographic: {
        gender?: string;
        identity_type?: string;
        identity_number?: string;
    }
    feilds?: Fields[];
}

export interface Location {
  latitude: number;
  longitude: number;
}

export interface Place {
  id: string;
  formattedAddress: string;
  displayName: {
    text: string;
  };
  location: Location;
  types: string[];
  rating?: number;
  userRatingCount?: number;
}

export interface SearchParams {
  location: Location;
  radius: number;
  max_results: number;
  text_query: string;
}

#!/usr/bin/env python3
import json
import sys

def convert_to_geojson(input_file, output_file):
    # Read the original zones.json file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Try to parse as JSON
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        print("The file appears to be corrupted. Please restore from backup.")
        sys.exit(1)
    
    # Check if already in GeoJSON format
    if data.get('type') == 'FeatureCollection':
        print("File is already partially in GeoJSON format")
        # Try to fix it if there are mixed formats
        if 'features' in data and 'data' not in data:
            # Already converted, just save it properly
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"File cleaned and saved to: {output_file}")
            return
    
    # Create GeoJSON FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Get the data array
    zones = data.get('data', data.get('features', []))
    
    # Convert each zone to a GeoJSON Feature
    for zone in zones:
        # Handle if already a Feature
        if zone.get('type') == 'Feature':
            geojson['features'].append(zone)
            continue
            
        # Convert coordinates from {lat, lng} to [lng, lat] (GeoJSON format)
        coordinates = []
        for coord in zone.get('coordinates', []):
            if isinstance(coord, dict):
                coordinates.append([coord['lng'], coord['lat']])
            elif isinstance(coord, list) and len(coord) == 2:
                coordinates.append(coord)
        
        if not coordinates:
            print(f"Warning: Zone {zone.get('id')} has no valid coordinates")
            continue
        
        # Create feature
        feature = {
            "type": "Feature",
            "id": zone.get('id'),
            "properties": {
                "id": zone.get('id'),
                "cityId": zone.get('cityId'),
                "cityAvailabilityStatus": zone.get('cityAvailabilityStatus'),
                "center": zone.get('center'),
                "modalities": zone.get('modalities', [])
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coordinates]
            }
        }
        
        geojson['features'].append(feature)
    
    # Save as GeoJSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully converted {len(geojson['features'])} zones to GeoJSON format")
    print(f"Output saved to: {output_file}")

if __name__ == '__main__':
    convert_to_geojson('zones.json', 'zones.geojson')

import math
from utils import *
from typing import List, Tuple

def inside(point: List[float], split_value: float, axis: str, side: str) -> bool:
    value = point[0] if axis == 'x' else point[1]
    if side in ['left', 'bottom']:
        return value <= split_value
    else:
        return value >= split_value

def compute_intersection(s: List[float], p: List[float], split_value: float, axis: str) -> List[float]:
    if axis == 'x':
        t = (split_value - s[0]) / (p[0] - s[0])
        y = s[1] + t * (p[1] - s[1])
        return [split_value, y]
    else:
        t = (split_value - s[1]) / (p[1] - s[1])
        x = s[0] + t * (p[0] - s[0])
        return [x, split_value]

def clip_polygon(polygon: List[List[float]], split_value: float, axis: str, side: str) -> List[List[List[float]]]:
    result = []
    s = polygon[-1]
    point_s = s
    
    for point in polygon:
        if inside(point, split_value, axis, side):
            if not inside(point_s, split_value, axis, side):
                intersection = compute_intersection(point_s, point, split_value, axis)
                result.append(intersection)
            result.append(point)
        elif inside(point_s, split_value, axis, side):
            intersection = compute_intersection(point_s, point, split_value, axis)
            result.append(intersection)
        point_s = point
    
    if result and result[0] != result[-1]:
        result.append(result[0])
    
    if len(result) < 4:
        raise ValueError("Inconsistent size: The polygon needs to have 4 points or more.")
    
    return [result]

def unwrap_coordinates(value: list) -> List[List[List[float]]]:
    coordinates = []
    for ring in value:
        if not isinstance(ring, list):
            continue
        unwrapped_ring = []
        for point in ring:
            if isinstance(point, list) and len(point) >= 2:
                x = point[0]
                y = point[1]
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    unwrapped_ring.append([x, y])
        if unwrapped_ring:
            coordinates.append(unwrapped_ring)
    return coordinates

def min_max_coordinate(coordinates: List[List[float]]) -> Tuple[float, float, float, float]:
    if not coordinates:
        raise ValueError("Inconsistent coordinates: Empty vector.")
    
    min_x = math.inf
    max_x = -math.inf
    min_y = math.inf
    max_y = -math.inf
    
    for point in coordinates:
        x, y = point[0], point[1]
        min_x = min(min_x, x)
        max_x = max(max_x, x)
        min_y = min(min_y, y)
        max_y = max(max_y, y)
    
    return min_x, max_x, min_y, max_y

def split_geometry(geometry: dict) -> Result[Tuple[dict, dict], str]:
    if geometry.get('type') != 'Polygon':
        raise ValueError("Inconsistent geometry type, this function only supports Polygon type.")
    
    coordinates = geometry.get('coordinates')
    if coordinates is None:
        raise ValueError("Inconsistent geometry: The key 'coordinates' doesn't exist.")
    
    unwrapped_coords = unwrap_coordinates(coordinates)
    if not unwrapped_coords:
        raise ValueError("Inconsistent geometry: Empty coordinates.")
    
    exterior = unwrapped_coords[0]
    min_x, max_x, min_y, max_y = min_max_coordinate(exterior)
    width = max_x - min_x
    height = max_y - min_y
    
    if width > height:
        split_value = (min_x + max_x) / 2
        poly1 = clip_polygon(exterior, split_value, 'x', 'left')
        poly2 = clip_polygon(exterior, split_value, 'x', 'right')
    else:
        split_value = (min_y + max_y) / 2
        poly1 = clip_polygon(exterior, split_value, 'y', 'bottom')
        poly2 = clip_polygon(exterior, split_value, 'y', 'top')
    
    geometry1 = {
        'type': 'Polygon',
        'coordinates': poly1
    }
    geometry2 = {
        'type': 'Polygon',
        'coordinates': poly2
    }
    
    return Ok((geometry1, geometry2))
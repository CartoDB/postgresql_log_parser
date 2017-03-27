import math
from decimal import Decimal


class GeoUtils(object):

    EARTH_RADIUS = 6378137
    EARTH_DIAMETER = EARTH_RADIUS * 2
    EARTH_CIRCUMFERENCE = EARTH_DIAMETER * math.pi
    TILE_SIZE_PIXEL = 256

    @classmethod
    def get_xyz_from_bbox(cls, xmin, ymin, xmax, ymax, metatile=False):
        initial_res = (cls.EARTH_CIRCUMFERENCE)/cls.TILE_SIZE_PIXEL
        origin_shift = (initial_res * cls.TILE_SIZE_PIXEL) / 2.0
        if metatile:
            zoom = GeoUtils.get_zoom_from_metatile_bbox(xmin, ymin, xmax, ymax)
        else:
            zoom = GeoUtils.get_zoom_from_tile_bbox(xmin, ymin, xmax, ymax)
        tile_geo_size = cls.TILE_SIZE_PIXEL * (initial_res / (math.pow(2, zoom)))
        x = round(Decimal((xmin + origin_shift)/tile_geo_size))
        y = round(Decimal(-((ymax - origin_shift)/tile_geo_size)))
        return {'x': x, 'y': y, 'z': zoom}

    @classmethod
    def get_zoom_from_metatile_bbox(cls, minx, miny, maxx, maxy):
        diff = maxx - minx
        return round(Decimal(math.log((cls.EARTH_CIRCUMFERENCE/(diff/2)), 2)))

    @classmethod
    def get_zoom_from_tile_bbox(cls, minx, miny, maxx, maxy):
        diff = maxx - minx
        return round(Decimal(math.log((cls.EARTH_CIRCUMFERENCE/diff), 2)))

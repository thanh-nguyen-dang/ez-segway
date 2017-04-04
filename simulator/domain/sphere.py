import math

class Sphere:
    @staticmethod
    def deg_to_rad(deg):
        return deg * (math.pi / 180.0)

    @staticmethod
    def haversine_value(lat1, lon1, lat2, lon2):
        rlat1 = Sphere.deg_to_rad(lat1)
        rlat2 = Sphere.deg_to_rad(lat2)
        rlon1 = Sphere.deg_to_rad(lon1)
        rlon2 = Sphere.deg_to_rad(lon2)
        dlat = math.fabs(rlat1 - rlat2)
        dlon = math.fabs(rlon1 - rlon2)
        h_value = math.sin(dlat/2) * math.sin(dlat/2) + \
            math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon/2) * math.sin(dlon/2)
        return h_value

    @staticmethod
    def distance(lat1, lon1, lat2, lon2):
        # implement Haversine formula
        # arcsin(x) = atan2(x, sqrt(1-x^2))
        h_value = Sphere.haversine_value(lat1, lon1, lat2, lon2)
        angle = 2 * math.atan2(math.sqrt(h_value), math.sqrt(1 - h_value))
        return 6371 * angle

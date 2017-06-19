import orca


@orca.column("parcel_data")
def total_employment(parcel_data):
    return parcel_data.local.emptot_p



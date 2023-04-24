package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if (queryRectangle == null|| pointString == null || queryRectangle.isEmpty()|| pointString.isEmpty())
      return false

    val rectangle_crdinates = queryRectangle.split(",")

    var x1 = rectangle_crdinates(0).toDouble

    var y1 = rectangle_crdinates(1).toDouble

    var x2 = rectangle_crdinates(2).toDouble

    var y2 = rectangle_crdinates(3).toDouble

    val internal_coords = pointString.split(",")

    var cord_x = internal_coords(0).toDouble

    var cord_y = internal_coords(1).toDouble

    if (cord_x >= x1 && cord_x <= x2 && cord_y >= y1 && cord_y <= y2)
      return true
    else if (cord_x >= x2 && cord_x <= x1 && cord_y >= y2 && cord_y <= y1)
      return true
    else
      return false
  }

  // YOU NEED TO CHANGE THIS PART

}

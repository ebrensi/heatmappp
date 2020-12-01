/*
 *  DrawBox represents the rectangular region that bounds all of our drawing on the canvas
 *  for a particular view. We use it to optimize clearing between redraws and moving a drawing
 *  after ViewBox recalibration.  Rather than clearing the whole screen for example, we only
 *  clear the DrawBox region which is more efficient.
 */

import * as ViewBox from "./ViewBox.js"

// Boundaries of the DrawBox in (absolute) px coordinates
let xmin, xmax, ymin, ymax

// Extra padding around the box, in Pixels.
const _pad = 10

// Return whether the DrawBox has any size
export function isEmpty() {
  return xmin === undefined
}

// Reset the DrawBox to empty
export function reset() {
  xmin = xmax = ymin = ymax = undefined
}

// Update the DrawBox dimensions with a Point in absolute rect coordinates
export function update(point) {
  const [x, y] = point

  if (!xmin || x < xmin) xmin = x
  if (!xmax || x > xmax) xmax = x
  if (!ymin || y < ymin) ymin = y
  if (!ymax || y > ymax) ymax = y
  return point
}


// The entire screen in screen coordinates.
// Sometimes when in doubt, it's safer to assume the DrawBox is the whole screen.
export function defaultRect() {
  const mapSize = ViewBox.getSize()
  return { x: 0, y: 0, w: mapSize.x, h: mapSize.y }
}

// The position/dimensions of the DrawBox in screen coordinates (0,0) is top-left
export function getScreenRect(pad) {
  pad = pad || _pad

  if (isEmpty()) return defaultRect()
  const mapSize = ViewBox.getSize()
  const transform = ViewBox.makeTransform()

  // upper-left corner
  const [Txmin, Tymin] = transform(xmin, ymin)
  const x = ~~Math.max(Txmin - pad, 0)
  const y = ~~Math.max(Tymin - pad, 0)

  // width and height
  const [Txmax, Tymax] = transform(xmax, ymax)
  const w = ~~Math.min(Txmax + pad, mapSize.x) - x
  const h = ~~Math.min(Tymax + pad, mapSize.y) - y

  return { x, y, w, h }
}

// Draw the outline of the DrawBox (or arbitrary rect object in screen coordinates)
export function draw(ctx, rect) {
  const { x, y, w, h } = rect || getScreenRect()
  ctx.strokeStyle = "rgb(0,255,0,0.8)"
  ctx.strokeRect(x, y, w, h)
}

// Clear the current DrawBox (or region given by rect object in screen coordinates)
export function clear(ctx, rect) {
  const { x, y, w, h } = rect || getScreenRect()
  ctx.clearRect(x, y, w, h)
}

// Copy the current DrawBox (or region given by rect object in screen coordinates)
// returns an ImageData object
export function copy(ctx, rect) {
  const { x, y, w, h } = rect || getScreenRect()
  return ctx.getImageData(x, y, w, h)
}

// write the contents of an ImageData object to the screen where the upper left-left
// corner of the rectangle is given by screenLoc
export function paste(ctx, imageData, screenLoc) {
  const { x, y } = screenLoc || getScreenRect()
  return ctx.putImageData(imageData, x, y)
}

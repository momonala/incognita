// Animated GPS trip-trace helpers shared by the live and GPS pages.
// Builds a deck.gl TripsLayer "comet" that replays a day (or date range) of trips.
(function (global) {
    const ORANGE = [255, 111, 0];
    const DEFAULT_LOOP_DURATION_S = 18;   // wall-clock seconds for one full replay loop
    const DEFAULT_TRAIL_LENGTH_S = 1800;  // length of the fading comet tail, in trip-seconds

    // Remap raw GPS timestamps to be continuous — strips idle gaps between segments so the
    // replay spends time proportional to motion, not to the hours parked between trips.
    function buildTripsData(dayPaths) {
        let cursor = 0;
        return dayPaths.map(path => {
            const rawTs = path.map(p => p[2]);
            const offset = cursor - rawTs[0];
            cursor += rawTs[rawTs.length - 1] - rawTs[0];
            return { path: path.map(p => [p[0], p[1]]), timestamps: rawTs.map(ts => ts + offset) };
        });
    }

    // Total remapped duration across all trips (the span the loop replays).
    function spanSeconds(tripsData) {
        return tripsData.reduce((s, t) => s + (t.timestamps.at(-1) - t.timestamps[0]), 0);
    }

    // Faint always-on outline of the full path, drawn under the animated trace.
    function makeGhostLayer(tripsData) {
        return new deck.PathLayer({
            id: 'ghost-path', data: tripsData, getPath: d => d.path,
            getColor: [...ORANGE, 110], getWidth: 2.5,
            widthUnits: 'pixels', rounded: true, pickable: false,
        });
    }

    function makeTripsLayer(tripsData, currentTime, trailLength) {
        return new deck.TripsLayer({
            id: 'trips-trace', data: tripsData,
            getPath: d => d.path, getTimestamps: d => d.timestamps,
            getColor: ORANGE, currentTime,
            trailLength, widthMinPixels: 3, rounded: true, pickable: false,
        });
    }

    global.TripsTrace = {
        ORANGE,
        DEFAULT_LOOP_DURATION_S,
        DEFAULT_TRAIL_LENGTH_S,
        buildTripsData,
        spanSeconds,
        makeGhostLayer,
        makeTripsLayer,
    };
})(window);

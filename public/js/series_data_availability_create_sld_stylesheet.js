function componentToHex(c) {
    var hex = c.toString(16);
    return hex.length == 1 ? "0" + hex : hex;
}
  
  function rgbToHex(r, g, b) {
    return "#" + componentToHex(r) + componentToHex(g) + componentToHex(b);
}
function createSldStyle(){
    var stylePars = { 
        'N': {fill:[110,110,110,1], radius: 8, stroke:[220,220,220,0], fontColor: "white", zIndex: 1, name: "no data", code: 0},
        'S': {fill: [110,110,110,1], radius: 7,stroke:[0,220,220,1], fontColor: "white", zIndex: 2, name: "simulated only", code: 1},
        'H': {fill: [0,128,255,1], radius: 8, stroke: [220,220,220,0], fontColor: "white", zIndex: 3, name: "historical data", code: 2},
        'H+S': {fill: [0,128,255,1], radius: 7, stroke:[0,220,220,1], fontColor: "white", zIndex: 4, name: "historical+simulated", code: 3},
        'C': {fill: [0,60,220,1], radius: 8, stroke: [220,220,220,0], fontColor: "white", zIndex: 5, name: "selected period", code: 4},
        'C+S': {fill: [0,60,220,1], radius: 7, stroke:[0,220,220,1], fontColor: "white", zIndex: 6, name: "selected period+simulated", code: 5},
        'NRT': {fill: [0,102,0,1], radius: 8, stroke: [220,220,220,0], fontColor: "white", zIndex: 7, name: "near real time", code: 6},
        'NRT+S': {fill: [0,102,0,1], radius: 7, stroke:[0,220,220,1], fontColor: "white", zIndex: 8, name: "near real time+simulated", code: 7},
        'RT': {fill: [0,255,0,1], radius: 8, stroke: [220,220,220,0], fontColor: "black", zIndex: 9, name: "real time", code: 8},
        'RT+S': {fill: [0,255,0,1], radius: 7, stroke:[0,220,220,1], fontColor: "black", zIndex: 10, name: "real time+simulated", code: 9}
    }
    var sld = ""
    for(var key of Object.keys(stylePars)) {
        const style = stylePars[key]
        sld += `<FeatureTypeStyle>
        <Rule>
            <Name>${key}</Name>
            <Title>${style.name}</Title>
            <ogc:Filter>
                <ogc:PropertyIsEqualTo>
                    <ogc:PropertyName>data_availability</ogc:PropertyName>
                    <ogc:Literal>${style.code}</ogc:Literal>
                </ogc:PropertyIsEqualTo>
            </ogc:Filter>
            <PointSymbolizer>
                <Graphic>
                    <Mark>
                        <WellKnownName>circle</WellKnownName>
                        <Fill>
                            <CssParameter name="fill">${rgbToHex(style.fill[0],style.fill[1],style.fill[2])}</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">${rgbToHex(style.stroke[0],style.stroke[1],style.stroke[2])}</CssParameter>
                            <CssParameter name="stroke-width">2</CssParameter>
                        </Stroke>
                    </Mark>
                    <Size>${style.radius}</Size>
                </Graphic>
            </PointSymbolizer>
        </Rule>
        </FeatureTypeStyle>
        `   
    }
    return `<?xml version="1.0" encoding="ISO-8859-1"?>
    <StyledLayerDescriptor version="1.0.0" 
        xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" 
        xmlns="http://www.opengis.net/sld" 
        xmlns:ogc="http://www.opengis.net/ogc" 
        xmlns:xlink="http://www.w3.org/1999/xlink" 
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <NamedLayer>
            <Name>Point coloured according to data_availability</Name>
            <UserStyle>
                <Title>Point coloured according to data_availability</Title>
                ${sld}
            </UserStyle>
        </NamedLayer>
    </StyledLayerDescriptor>`
}

console.log(createSldStyle())
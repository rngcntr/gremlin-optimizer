package de.rngcntr.gremlin.optimize.filter;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElementFilterTests {

    @ParameterizedTest
    @ValueSource(classes = {
            Vertex.class,
            Edge.class
    })
    public void testConstructor(Class<? extends Element> clazz) {
        ElementFilter<?> f = Mockito.mock(ElementFilter.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertEquals(clazz, f.getFilteredType());
    }
}

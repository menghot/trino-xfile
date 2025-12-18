//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.trino.sql.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class QualifiedName {
    private final List<Identifier> originalParts;
    private final List<String> parts;
    private final String name;
    private final Optional<QualifiedName> prefix;
    private final String suffix;

    public static QualifiedName of(String first, String... rest) {
        Objects.requireNonNull(first, "first is null");
        return of(Lists.asList(first, rest).stream().map(Identifier::new).collect(ImmutableList.toImmutableList()));
    }

    public static QualifiedName of(String name) {
        Objects.requireNonNull(name, "name is null");
        return of(ImmutableList.of(new Identifier(name)));
    }

    public static QualifiedName of(Iterable<Identifier> originalParts) {
        Objects.requireNonNull(originalParts, "originalParts is null");
        Preconditions.checkArgument(!Iterables.isEmpty(originalParts), "originalParts is empty");
        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    private QualifiedName(List<Identifier> originalParts) {
        this.originalParts = originalParts;
        ImmutableList.Builder<String> partsBuilder = ImmutableList.builderWithExpectedSize(originalParts.size());

        for (Identifier identifier : originalParts) {
            partsBuilder.add(mapIdentifier(identifier));
        }

        this.parts = partsBuilder.build();
        this.name = String.join(".", this.parts);
        if (originalParts.size() == 1) {
            this.prefix = Optional.empty();
            this.suffix = mapIdentifier(originalParts.get(0));
        } else {
            List<Identifier> subList = originalParts.subList(0, originalParts.size() - 1);
            this.prefix = Optional.of(new QualifiedName(subList));
            this.suffix = mapIdentifier(originalParts.getLast());
        }

    }

    private static String mapIdentifier(Identifier identifier) {
        return identifier.getValue();
    }

    public List<String> getParts() {
        return this.parts;
    }

    public List<Identifier> getOriginalParts() {
        return this.originalParts;
    }

    public Optional<QualifiedName> getPrefix() {
        return this.prefix;
    }

    public boolean hasSuffix(QualifiedName suffix) {
        if (this.parts.size() < suffix.getParts().size()) {
            return false;
        } else {
            int start = this.parts.size() - suffix.getParts().size();
            return this.parts.subList(start, this.parts.size()).equals(suffix.getParts());
        }
    }

    public String getSuffix() {
        return this.suffix;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else {
            return o != null && this.getClass() == o.getClass() && this.parts.equals(((QualifiedName) o).parts);
        }
    }

    public int hashCode() {
        return this.parts.hashCode();
    }

    public String toString() {
        return this.name;
    }
}

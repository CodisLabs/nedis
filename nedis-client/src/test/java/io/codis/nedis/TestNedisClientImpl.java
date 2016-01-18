/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://
import org.junit.Test;
www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import java.io.IOException;
import java.util.Set;

import org.junit.Test;
import org.mockito.asm.AnnotationVisitor;
import org.mockito.asm.Attribute;
import org.mockito.asm.ClassReader;
import org.mockito.asm.ClassVisitor;
import org.mockito.asm.FieldVisitor;
import org.mockito.asm.Label;
import org.mockito.asm.MethodVisitor;
import org.mockito.asm.Opcodes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.codis.nedis.NedisClientImpl;

/**
 * At least make sure that we call the right commands.
 * 
 * @author Apache9
 */
public class TestNedisClientImpl {

    private static final Set<String> EXCLUDE_METHODS = ImmutableSet.<String>builder()
            .add("setTimeout").add("eventLoop").add("isOpen").add("release").add("closeFuture")
            .add("close").add("execCmd").add("execCmd0").add("execScanCmd").add("execTxnCmd")
            .add("exec").add("auth").add("quit").add("select").add("clientSetname").build();

    private static final ClassVisitor CV = new ClassVisitor() {

        @Override
        public void visitSource(String source, String debug) {}

        @Override
        public void visitOuterClass(String owner, String name, String desc) {}

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature,
                String[] exceptions) {
            if ((Opcodes.ACC_PUBLIC & access) == 0 || EXCLUDE_METHODS.contains(name)
                    || name.contains("<")) {
                return null;
            }
            if (name.endsWith("0")) {
                name = name.substring(0, name.length() - 1);
            }
            int index = -1;
            for (int i = 0; i < name.length(); i++) {
                if (Character.isUpperCase(name.charAt(i))) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return new FindEnumVisitor(name, name.toUpperCase(), null);
            } else {
                String cmd = name.substring(0, index).toUpperCase();
                return new FindEnumVisitor(name, cmd, name.substring(index).toUpperCase());
            }
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {}

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature,
                Object value) {
            return null;
        }

        @Override
        public void visitEnd() {}

        @Override
        public void visitAttribute(Attribute attr) {}

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName,
                String[] interfaces) {}
    };

    private static final class FindEnumVisitor implements MethodVisitor {

        private final String method;

        private final String cmd;

        private final Set<String> visitedCmds = Sets.newHashSet();

        private final String keyword;

        private final Set<String> visitedKeywords = Sets.newHashSet();

        public FindEnumVisitor(String method, String cmd, String keyword) {
            this.method = method;
            this.cmd = cmd;
            this.keyword = keyword;
        }

        @Override
        public AnnotationVisitor visitAnnotationDefault() {
            return null;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc,
                boolean visible) {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {}

        @Override
        public void visitCode() {}

        @Override
        public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {}

        @Override
        public void visitInsn(int opcode) {}

        @Override
        public void visitIntInsn(int opcode, int operand) {}

        @Override
        public void visitVarInsn(int opcode, int var) {}

        @Override
        public void visitTypeInsn(int opcode, String type) {}

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
            if (opcode != Opcodes.GETSTATIC) {
                return;
            }
            if (owner.endsWith("RedisCommand")) {
                visitedCmds.add(name);
            } else if (owner.endsWith("RedisKeyword")) {
                visitedKeywords.add(name);
            }
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc) {}

        @Override
        public void visitJumpInsn(int opcode, Label label) {}

        @Override
        public void visitLabel(Label label) {}

        @Override
        public void visitLdcInsn(Object cst) {}

        @Override
        public void visitIincInsn(int var, int increment) {}

        @Override
        public void visitTableSwitchInsn(int min, int max, Label dflt, Label[] labels) {}

        @Override
        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels) {}

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {}

        @Override
        public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {}

        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start,
                Label end, int index) {}

        @Override
        public void visitLineNumber(int line, Label start) {}

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {}

        @Override
        public void visitEnd() {
            assertThat(method + " miss command enum", visitedCmds,
                    is((Set<String>) Sets.newHashSet(cmd)));
            if (keyword != null) {
                assertThat(method + " miss keyword enum", visitedKeywords, hasItem(keyword));
            }
        }
    }

    @Test
    public void assertCommandEnum() throws IOException {
        ClassReader cr = new ClassReader(NedisClientImpl.class.getName());
        cr.accept(CV, ClassReader.SKIP_DEBUG);
    }
}

/********************************************************************

  Copyright 2012 Konstantin Olkhovskiy <lupus@oxnull.net>

  This file is part of Mersenne.

  Mersenne is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  any later version.

  Mersenne is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Mersenne.  If not, see <http://www.gnu.org/licenses/>.

 ********************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <err.h>
#include <clang-c/Index.h>

FILE *header, *source;

enum CXChildVisitResult enum_constant_finder(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	CXString string;
	if(CXCursor_EnumConstantDecl == clang_getCursorKind(cursor)) {
		string = clang_getCursorSpelling(cursor);
		fprintf(source, "\t\tcase %s: return \"%s\";\n", clang_getCString(string), clang_getCString(string));
		clang_disposeString(string);
		return CXChildVisit_Continue;
	}
	return CXChildVisit_Recurse;
}

enum CXChildVisitResult enum_finder(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	CXString enum_name;
	CXString file_name;
	CXFile file;
	CXSourceLocation location;

	if(CXCursor_EnumDecl == clang_getCursorKind(cursor)) {
		location = clang_getCursorLocation(cursor);
		clang_getInstantiationLocation(location, &file, NULL, NULL, NULL);
		file_name = clang_getFileName(file);
		if(strcmp((const char *)client_data, clang_getCString(file_name))) {
			clang_disposeString(file_name);
			return CXChildVisit_Continue;
		}
		enum_name = clang_getCursorSpelling(cursor);
		fprintf(header, "const char * strval_%s(enum %s val);\n", clang_getCString(enum_name), clang_getCString(enum_name));
		fprintf(source, "const char * strval_%s(enum %s val) {\n", clang_getCString(enum_name), clang_getCString(enum_name));
		fprintf(source, "\tswitch(val) {\n");
		clang_visitChildren(cursor, enum_constant_finder, NULL);
		fprintf(source, "\t}\treturn (char*)0;\n\n}\n\n");
		clang_disposeString(enum_name);
		clang_disposeString(file_name);
		return CXChildVisit_Continue;
	}
	return CXChildVisit_Recurse;
}

char * replace_ext(const char *source, const char *suffix)
{
	char *replaced;
	int last_dot = (strrchr(source, '.') - source);
	int slen = strlen(suffix);
	replaced = malloc(last_dot + slen + 1);
	memcpy(replaced, source, last_dot);
	memcpy(replaced + last_dot, suffix, slen);
	replaced[last_dot + slen] = '\0';
	return replaced;
}

int main(int argc, char *argv[]) {
	int i, n;
	CXCursor cursor;
	CXIndex index;
	CXTranslationUnit tu;
	CXDiagnostic diag;
	CXString string;
	CXString file_name;
	char *source_name, *header_name;

	index = clang_createIndex(0, 0);
	tu = clang_parseTranslationUnit(index, 0,
			(const char *const *)argv, argc, 0, 0,
			CXTranslationUnit_None);
	for (i = 0, n = clang_getNumDiagnostics(tu); i != n; i++) {
		diag = clang_getDiagnostic(tu, i);
		string = clang_formatDiagnostic(diag,
				clang_defaultDiagnosticDisplayOptions());
		fprintf(stderr, "%s\n", clang_getCString(string));
		clang_disposeString(string);
	}
	cursor = clang_getTranslationUnitCursor(tu);
	file_name = clang_getTranslationUnitSpelling(tu);
	header_name = replace_ext(basename((char *)clang_getCString(file_name)), ".strenum.h");
	source_name = replace_ext(basename((char *)clang_getCString(file_name)), ".strenum.c");
	header = fopen(header_name, "w+");
	if(!header)
		err(EXIT_FAILURE, "fopen");
	source = fopen(source_name, "w+");
	if(!header)
		err(EXIT_FAILURE, "fopen");
	fprintf(header, "#include \"%s\"\n\n", clang_getCString(file_name));
	fprintf(source, "#include \"%s\"\n\n", header_name);
	clang_visitChildren(cursor, enum_finder, (CXClientData)clang_getCString(file_name));
	fclose(header);
	fclose(source);
	clang_disposeTranslationUnit(tu);
	clang_disposeIndex(index);
	return 0;
}

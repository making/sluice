# python3 undef.py <console.js> [kit.js]: calls of names neither defined nor imported (rough),
# and with kit.js the imports it does not export; imports unused either way.
import re,sys
def strip(src):
    code=re.sub(r'//[^\n]*|/\*.*?\*/','',src,flags=re.S)
    # a string goes; a template leaves what it interpolates
    held=lambda m: ' '.join(re.findall(r'\$\{([^{}]*)\}',m.group(0))) if m.group(0)[0]=='`' else '""'
    return re.sub(r'`(?:[^`\\]|\\.)*`|"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'',held,code).replace('...',' ')
src=open(sys.argv[1]).read()
imp=re.search(r'import\s*\{([^}]*)\}',src)
imports={x.strip() for x in imp.group(1).split(',') if x.strip()} if imp else set()
code=strip(src[imp.end():] if imp else src)
defined=set(re.findall(r'\b(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)',code))
defined|=set(re.findall(r'\(([^()]*)\)\s*=>',code) and [p.strip().split('=')[0].strip() for g in re.findall(r'\(([^()]*)\)\s*=>',code) for p in g.split(',')])
defined|=set(re.findall(r'\b([A-Za-z_$][\w$]*)\s*=>',code))
defined|=imports
for m in re.finditer(r'(?:const|let)\s*\{([^}]*)\}',code): defined|={x.split(':')[-1].strip() for x in m.group(1).split(',')}
for m in re.finditer(r'function\s*[\w$]*\s*\(([^)]*)\)',code): defined|={x.split('=')[0].strip() for x in m.group(1).split(',')}
for m in re.finditer(r'for\s*\((?:const|let)\s*(\[[^\]]*\]|[\w$]+)',code): defined|=set(re.findall(r'[\w$]+',m.group(1)))
for m in re.finditer(r'catch\s*\(([\w$]+)\)',code): defined.add(m.group(1))
glob=set('document window location fetch setTimeout clearTimeout setInterval Promise Date Math JSON Number String Object Array Map Set Error Node Intl XMLHttpRequest AbortController CSS getSelection navigator requestAnimationFrame URLSearchParams encodeURIComponent decodeURIComponent Boolean parseInt isNaN Uint8Array RegExp console structuredClone queueMicrotask Infinity undefined null true false this new return typeof instanceof if else for while switch case default break continue throw try catch finally await async of in do void delete import from export yield super crypto confirm matchMedia'.split())
calls=set(re.findall(r'(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(',code))
print('calls undefined:',sorted(n for n in calls if n not in defined and n not in glob))
used=set(re.findall(r'(?<![.\w$])([A-Za-z_$][\w$]*)\b',code))
print('imports unused:',sorted(imports-used))
if len(sys.argv)>2:
    exports=set(re.findall(r'^export\s+(?:async\s+)?(?:function|const|let|class)\s+([\w$]+)',open(sys.argv[2]).read(),flags=re.M))
    print('imports not exported:',sorted(imports-exports))

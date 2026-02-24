const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const inputFile = 'README.md';
const outputFile = 'README-processed.md';
const diagramDir = 'diagrams';

if (!fs.existsSync(diagramDir)) {
    fs.mkdirSync(diagramDir);
}

let content = fs.readFileSync(inputFile, 'utf8');
const mermaidRegex = /```mermaid\n([\s\S]*?)```/g;

let match;
let diagramCount = 0;
const replacements = [];

while ((match = mermaidRegex.exec(content)) !== null) {
    diagramCount++;
    const mermaidCode = match[1].trim();
    const diagramName = `diagram-${diagramCount}`;
    const mmdFile = path.join(diagramDir, `${diagramName}.mmd`);
    const pngFile = path.join(diagramDir, `${diagramName}.png`);

    fs.writeFileSync(mmdFile, mermaidCode);

    console.log(`Rendering diagram ${diagramCount}...`);
    
    try {
        execSync(`npx mmdc -i "${mmdFile}" -o "${pngFile}" -t dark -b transparent`, {
            stdio: 'inherit'
        });
    } catch (e) {
        console.error(`Failed to render diagram ${diagramCount}`);
    }

    replacements.push({
        original: match[0],
        replacement: `![${diagramName}](${pngFile})`
    });
}

replacements.forEach(({ original, replacement }) => {
    content = content.replace(original, replacement);
});

fs.writeFileSync(outputFile, content);

console.log(`\n✓ Processed ${diagramCount} mermaid diagram(s)`);
console.log(`✓ Output written to ${outputFile}`);
console.log(`✓ Diagrams saved to ${diagramDir}/`);

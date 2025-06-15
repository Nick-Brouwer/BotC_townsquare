const fs = require("fs");
const https = require("https");
const WebSocket = require("ws");
const client = require("prom-client");
const nbt = require('prismarine-nbt');
const archiver = require("archiver");
const path = require("path");

// Create a Registry which registers the metrics
const register = new client.Registry();
// Add a default label which is added to all metrics
register.setDefaultLabels({
  app: "clocktower-online"
});

const PING_INTERVAL = 30000; // 30 seconds

const options = {};

if (process.env.NODE_ENV !== "development") {
  options.cert = fs.readFileSync("cert.pem");
  options.key = fs.readFileSync("key.pem");
}

const server = https.createServer(options);
const wss = new WebSocket.Server({
  ...(process.env.NODE_ENV === "development" ? { port: 8081 } : { server }),
  verifyClient: info =>
    info.origin &&
    !!info.origin.match(
      /^https?:\/\/([^.]+\.github\.io|localhost|clocktower\.online|eddbra1nprivatetownsquare\.xyz)/i
    )
});

function noop() {}

// calculate latency on heartbeat
function heartbeat() {
  this.latency = Math.round((new Date().getTime() - this.pingStart) / 2);
  this.counter = 0;
  this.isAlive = true;
}

// map of channels currently in use
const channels = {};

// metrics
const metrics = {
  players_concurrent: new client.Gauge({
    name: "players_concurrent",
    help: "Concurrent Players",
    collect() {
      this.set(wss.clients.size);
    }
  }),
  channels_concurrent: new client.Gauge({
    name: "channels_concurrent",
    help: "Concurrent Channels",
    collect() {
      this.set(Object.keys(channels).length);
    }
  }),
  channels_list: new client.Gauge({
    name: "channel_players",
    help: "Players in each channel",
    labelNames: ["name"],
    collect() {
      for (let channel in channels) {
        this.set(
          { name: channel },
          channels[channel].filter(
            ws =>
              ws &&
              (ws.readyState === WebSocket.OPEN ||
                ws.readyState === WebSocket.CONNECTING)
          ).length
        );
      }
    }
  }),
  messages_incoming: new client.Counter({
    name: "messages_incoming",
    help: "Incoming messages"
  }),
  messages_outgoing: new client.Counter({
    name: "messages_outgoing",
    help: "Outgoing messages"
  }),
  connection_terminated_host: new client.Counter({
    name: "connection_terminated_host",
    help: "Terminated connection due to host already present"
  }),
  connection_terminated_spam: new client.Counter({
    name: "connection_terminated_spam",
    help: "Terminated connection due to message spam"
  }),
  connection_terminated_timeout: new client.Counter({
    name: "connection_terminated_timeout",
    help: "Terminated connection due to timeout"
  })
};

// register metrics
for (let metric in metrics) {
  register.registerMetric(metrics[metric]);
}

// a new client connects
wss.on("connection", function connection(ws, req) {
  // url pattern: clocktower.online/<channel>/<playerId|host>
  const url = req.url.toLocaleLowerCase().split("/");
  ws.playerId = url.pop();
  ws.channel = url.pop();
  // check for another host on this channel
  if (
    ws.playerId === "host" &&
    channels[ws.channel] &&
    channels[ws.channel].some(
      client =>
        client !== ws &&
        client.readyState === WebSocket.OPEN &&
        client.playerId === "host"
    )
  ) {
    console.log(ws.channel, "duplicate host");
    ws.close(1000, `The channel "${ws.channel}" already has a host`);
    metrics.connection_terminated_host.inc();
    return;
  }
  ws.isAlive = true;
  ws.pingStart = new Date().getTime();
  ws.counter = 0;
  // add channel to list
  if (!channels[ws.channel]) {
    channels[ws.channel] = [];
  }
  channels[ws.channel].push(ws);
  // start ping pong
  ws.ping(noop);
  ws.on("pong", heartbeat);
  // handle message
  ws.on("message", function incoming(data) {
    metrics.messages_incoming.inc();
    // check rate limit (max 5msg/second)
    ws.counter++;
    if (ws.counter > (5 * PING_INTERVAL) / 1000) {
      console.log(ws.channel, "disconnecting user due to spam");
      ws.close(
        1000,
        "Your app seems to be malfunctioning, please clear your browser cache."
      );
      metrics.connection_terminated_spam.inc();
      return;
    }
    const messageType = data
      .toLocaleLowerCase()
      .substr(1)
      .split(",", 1)
      .pop();
    switch (messageType) {
      case '"ping"':
        // ping messages will only be sent host -> all or all -> host
        channels[ws.channel].forEach(function each(client) {
          if (
            client !== ws &&
            client.readyState === WebSocket.OPEN &&
            (ws.playerId === "host" || client.playerId === "host")
          ) {
            client.send(
              data.replace(/latency/, (client.latency || 0) + (ws.latency || 0))
            );
            metrics.messages_outgoing.inc();
          }
        });
        break;
      case '"direct"':
        // handle "direct" messages differently
        console.log(
          new Date(),
          wss.clients.size,
          ws.channel,
          ws.playerId,
          data
        );
        try {
          const dataToPlayer = JSON.parse(data)[1];
          channels[ws.channel].forEach(function each(client) {
            if (
              client !== ws &&
              client.readyState === WebSocket.OPEN &&
              dataToPlayer[client.playerId]
            ) {
              client.send(JSON.stringify(dataToPlayer[client.playerId]));
              metrics.messages_outgoing.inc();
            }
          });
        } catch (e) {
          console.log("error parsing direct message JSON", e);
        }
        break;
      default:
        // all other messages
        console.log(
          new Date(),
          wss.clients.size,
          ws.channel,
          ws.playerId,
          data
        );
        channels[ws.channel].forEach(function each(client) {
          if (client !== ws && client.readyState === WebSocket.OPEN) {
            client.send(data);
            metrics.messages_outgoing.inc();
          }
        });
        break;
    }
  });
});

// start ping interval timer
const interval = setInterval(function ping() {
  // ping each client
  wss.clients.forEach(function each(ws) {
    if (ws.isAlive === false) {
      metrics.connection_terminated_timeout.inc();
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.pingStart = new Date().getTime();
    ws.ping(noop);
  });
  // clean up empty channels
  for (let channel in channels) {
    if (
      !channels[channel].length ||
      !channels[channel].some(
        ws =>
          ws &&
          (ws.readyState === WebSocket.OPEN ||
            ws.readyState === WebSocket.CONNECTING)
      )
    ) {
      metrics.channels_list.remove({ name: channel });
      delete channels[channel];
    }
  }
}, PING_INTERVAL);

// handle server shutdown
wss.on("close", function close() {
  clearInterval(interval);
});

// prod mode with stats API
if (process.env.NODE_ENV !== "development") {
  console.log("server starting");
  server.listen(8080, () => {
    console.log("Server is now listening on port 8080");
  });
    server.on("request", async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") {
      // Respond to preflight request quickly
      res.writeHead(204);
      res.end();
      return;
    }
    
    if (req.url === "/api/players") {
      try {
        const players = await readPlayerSeats();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(players));
      } catch (err) {
        res.writeHead(500);
        res.end(JSON.stringify({ error: "Failed to read player seats" }));
        console.error(err);
      }
      return;
    }

    if (req.url === "/api/exportRoles" && req.method === "POST") {
      let body = "";
      req.on("data", chunk => (body += chunk));
      req.on("end", async () => {
        try {
          const roles = JSON.parse(body)
          generateDatapack(roles);

          // const roles = JSON.parse(body);
          // await exportRoles(roles);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "success" }));
        } catch (err) {
          res.writeHead(500);
          res.end(JSON.stringify({ error: "Failed to export roles" }));
          console.error(err);
        }
      });
      return;
    }

    res.setHeader("Content-Type", register.contentType);
    register.metrics().then(out => res.end(out));
  });
}

async function exportRoles(roles) {
  const exportPath = "D:/Users/Nick/Downloads/role_assignments.json";
  await fs.promises.writeFile(exportPath, JSON.stringify(roles, null, 2), "utf-8");
}

async function readPlayerSeats() {
  const downloadsDir = "D:/Users/Nick/Downloads";
  const files = await fs.promises.readdir(downloadsDir);

  const datFiles = files
    .filter(file => file.startsWith("scoreboard") && file.endsWith(".dat"))
    .map(file => path.join(downloadsDir, file));

  // Sort by modified time (most recent first)
  const filesWithStats = await Promise.all(datFiles.map(async file => ({
    file,
    mtime: (await fs.promises.stat(file)).mtime
  })));

  filesWithStats.sort((a, b) => b.mtime - a.mtime);

  if (filesWithStats.length === 0) {
    throw new Error("No scoreboard .dat files found.");
  }

  const filePath = filesWithStats[0].file;

  const data = await fs.promises.readFile(filePath)
  const parsed = await nbt.parse(data)

  // This depends on your actual file structure, but from your Python code:
  // 'data' is the root tag, 'PlayerScores' is inside data, etc.
  const root = parsed.parsed.value

  // Find the PlayerScores list under root['data']['PlayerScores']
  const playerScoresList = root.data.value.PlayerScores.value.value;

  // Extract players with Objective "Player"
  const players = playerScoresList
    .filter(entry => entry.Objective.value === 'Player')
    .map(entry => ({
    name: entry.Name.value,
    seat: Math.abs(entry.Score.value)
    }))

  // Sort by seat ascending
  players.sort((a, b) => a.seat - b.seat)

  return players
}

function escapeForMinecraftJSON(text) {
  return text
    .replace(/\\/g, '\\\\')  // escape backslashes
    .replace(/"/g, '\\"')    // escape double quotes
    .replace(/\n/g, '\\\\n');  // convert newlines to literal \n
}

function createBookInsertCommand(player, slot = 13) {
  const coords = chestCoordinates[player.id];
  if (!coords) return ""; // Skip if no coordinates

  const [x, y, z] = coords;

  // Determine color based on team
  let color;
  if (player.role.team === "townsfolk" || player.role.team === "outsider") {
    color = "blue";
  } else if (player.role.team === "minion" || player.role.team === "demon") {
    color = "dark_red";
  } else {
    color = "black";
  }

  const role = escapeForMinecraftJSON(player.role.name);

  const team = player.role.team;
  const capitalizedTeam = team.charAt(0).toUpperCase() + team.slice(1);

  const roleWithSymbol = getRoleWithEmoji(player.role.name).replace(/\uFE0F/g, '');;
  const customName = `[{"text":"${roleWithSymbol} - ${capitalizedTeam}","italic":false,"color":"${color}"}]`;

  const ability = escapeForMinecraftJSON(player.role.ability);

  const pageJSON = [
    `"You are the\\\\n"`,
    `{"text":"${role}","color":"${color}"}`,
    `"\\\\n\\\\n"`,
    `{"text":"${ability}","color":"black"}`
  ];

  const loreAbility = splitTextToLoreComponents(ability);

  const pageString = `'[${pageJSON.join(",")}]'`;
  
  return `data modify block ${x} ${y} ${z} Items set value [{Slot:${slot},id:"minecraft:written_book",Count:1,components:{written_book_content:{title:"${role}",pages:[${pageString}]},custom_data:{role_book:1},lore:${loreAbility},custom_name:'${customName}'}}]`;
}

function createPlaceholderBookInsertCommand(id, slot = 13) {
  const coords = chestCoordinates[id];
  if (!coords) return ""; // Skip if no coordinates

  const [x, y, z] = coords;
  const title = `No Role #${id}`;
  const text = `{"text":"Uninhabited house"}`;

  return `data modify block ${x} ${y} ${z} Items set value [{Slot:${slot},id:"minecraft:written_book",Count:1,components:{written_book_content:{title:"${title}",author:"Storyteller",pages:['${text}']},custom_data:{role_book:1}}}]`;
}

function createVisitOrderBook(players, nightType = true) {
  const nightTypeString = nightType ? "firstNight" : "otherNight";  
  
  const filtered = players
    .filter(p => {
      const priority = p.role[nightTypeString];
      const team = p.role.team;
      return (
        typeof priority === "number" &&
        (priority !== 0 || team === "demon" || team === "minion")
      );
    })
    .map(p => ({
      id: p.id,
      name: p.name,
      role: p.role.name,
      team: p.role.team,
      priority: p.role[nightTypeString]
    }));

  const order = filtered.sort((a, b) => a.priority - b.priority);

  const pages = [];
  let pageJson = [];

    // Add the header line to the first page
    pageJson.push({
      text: nightType ? "First night order:\\n\\n" : "Other nights order:\\n\\n"
    });

  for (let i = 0; i < order.length; i++) {
    const p = order[i];
    const coords = teleportCoordinates[p.id];
    if (!coords) continue; // skip if no coords

    const paddedId = p.id.toString().padStart(2, "0");
    const lineText = `${paddedId}- ${p.role}\\n`;

    const lineComponent = {
      text: lineText,
      color: "black",
      bold: true,
      clickEvent: {
        action: "run_command",
        value: `/tp @s ${coords[0]} ${coords[1]} ${coords[2]}`
      }
    };

    const currentLengthRoles = pageJson.reduce((acc, cur) => acc + (cur.text?.length || 0), 0);
    if (currentLengthRoles + lineText.length > 220) {
      pages.push(JSON.stringify({ text: "", extra: pageJson }));
      pageJson = [];
    }

    pageJson.push(lineComponent);
  }
  
  // Add teleport to Town Square
  const townCoords = teleportCoordinates.townSquare;
  const townSquareLine = {
    text: "Teleport to Town Square\\n",
    color: "gold",
    bold: true,
    clickEvent: {
      action: "run_command",
      value: `/tp @s ${townCoords[0]} ${townCoords[1]} ${townCoords[2]}`
    }
  };

  const currentLength = pageJson.reduce((acc, cur) => acc + (cur.text?.length || 0), 0);
  if (currentLength + townSquareLine.text.length > 220) {
    pages.push(JSON.stringify({ text: "", extra: pageJson }));
    pageJson = [];
  }

  pageJson.push(townSquareLine);

  if (pageJson.length > 0) {
    pages.push(JSON.stringify({ text: "", extra: pageJson }));
  }

  return pages;
}

function createVisitOrderBookCommand(players, firstNight)
{
  const title = firstNight ? "First Night" : "Other Nights";
  const pages = createVisitOrderBook(players, firstNight);
  const joinedPages = `['${pages.join(",")}']`;

  return `give @s minecraft:written_book[minecraft:written_book_content={title:"${title}",author:"Storyteller",pages:${joinedPages}},custom_data={role_book:1}]`;
}

// Create datapack structure
function generateDatapack(players) {
  const exportPath = "D:/Users/Nick/Downloads";
  const basePath = path.join(exportPath, 'botc_datapack');
  const funcPath = path.join(basePath, 'data', 'botc', 'function');
  const metaPath = path.join(basePath, 'pack.mcmeta');

  fs.mkdirSync(funcPath, { recursive: true });

  // Create an array of commands first (don't join yet)
  const commands = [
    'clear @a minecraft:written_book[minecraft:custom_data={role_book:1}]',
    ...players.map((p, i) => createBookInsertCommand(p, 13)).filter(Boolean)
  ];

  // Add placeholders for missing players
  const usedIds = new Set(players.map(p => p.id));
  for (let id = 1; id <= 12; id++) {
    if (!usedIds.has(id)) {
      const placeholderCmd = createPlaceholderBookInsertCommand(id, 13);
      if (placeholderCmd) commands.push(placeholderCmd);
    }
  }

  // Join only when writing the file
  // fs.writeFileSync(path.join(funcPath, 'give_books.mcfunction'), commands.join('\n'), 'utf8');
  const storytellerBooks = [
    createVisitOrderBookCommand(players, true),
    createVisitOrderBookCommand(players, false)
  ];

  fs.writeFileSync(
    path.join(funcPath, 'give_books.mcfunction'),
    [...commands, ...storytellerBooks].join('\n'),
    'utf8'
  );

  // Create 12 placeholder book commands, one for each seat ID 1-12
  const placeholderCommands = [];
  for (let id = 1; id <= 12; id++) {
    const cmd = createPlaceholderBookInsertCommand(id, 13);
    if (cmd) placeholderCommands.push(cmd);
  }
  // Write placeholder book commands to a separate function file
  fs.writeFileSync(
    path.join(funcPath, 'reset_books.mcfunction'),
    ['clear @a minecraft:written_book[minecraft:custom_data={role_book:1}]', ...placeholderCommands].join('\n'),
    'utf8'
  );

  // Create pack.mcmeta
  const mcmeta = {
    pack: {
      pack_format: 48,
      description: "Blood on the Clocktower Role Books"
    }
  };
  fs.writeFileSync(metaPath, JSON.stringify(mcmeta, null, 2), 'utf8');

  // Create a zip file
  const output = fs.createWriteStream(path.join(exportPath, 'botc_datapack.zip'));
  const archive = archiver('zip', { zlib: { level: 9 } });

  output.on('close', () => {
    console.log(`Datapack zipped (${archive.pointer()} bytes), now cleaning up...`);
    fs.rmSync(basePath, { recursive: true, force: true });
    console.log('Datapack folder cleaned up.');
  });

  archive.on('error', err => {
    throw err;
  });

  archive.pipe(output);
  archive.directory(basePath, false); // Zip contents of botc_datapack folder
  archive.finalize();
}

function splitTextToLoreComponents(text, maxLength = 40) {
  const words = text.split(' ');
  const lines = [];
  let currentLine = '';

  for (const word of words) {
    if ((currentLine + word).length + (currentLine ? 1 : 0) > maxLength) {
      lines.push(currentLine);
      currentLine = word;
    } else {
      currentLine += (currentLine ? ' ' : '') + word;
    }
  }

  if (currentLine) lines.push(currentLine);

  // Convert to lore components
  const loreComponents = lines.map(line => `'{"text":"${line}","italic":false}'`);
  return `[${loreComponents.join(',')}]`;
}

const chestCoordinates = {
  1: [243, 96, 33],
  2: [233, 99, 54],
  3: [212, 95, 45],
  4: [157, 94, 55],
  5: [134, 95, 72],
  6: [114, 93, 53],
  7: [112, 95, -38],
  8: [124, 95, -61],
  9: [154, 96, -68],
  10: [216, 95, -28],
  11: [243, 96, -49],
  12: [254, 96, -17]
};

function getRoleWithEmoji(roleName) {
  const emoji = roleEmojis[roleName];
  return emoji ? `${roleName} ${emoji}` : roleName;
}

const teleportCoordinates = {
  1: [239.46, 96.00, 32.51],
  2: [235.57, 99.00, 48.41],
  3: [211.47, 95.00, 41.63],
  4: [153.41, 94.06, 57.46],
  5: [133.11, 95.06, 69.52],
  6: [119.68, 93.00, 47.04],
  7: [116.60, 95.06, -39.98],
  8: [126.63, 95.00, -58.30],
  9: [158.54, 96.00, -64.53],
  10: [222.61, 95.00, -32.48],
  11: [246.50, 96.00, -46.30],
  12: [246.40, 96.00, -19.50],
  roles: [196, 84, 6],
  townSquare: [167.51, 92.00, -3.54]
};

const roleEmojis = {
  // Trouble Brewing
  "Washerwoman": "👕",
  "Librarian": "📚",
  "Investigator": "🔎",
  "Chef": "👨‍🍳",
  "Empath": "❤️",
  "Fortune Teller": "🔮",
  "Undertaker": "⚰️",
  "Monk": "✝️",
  "Ravenkeeper": "🐦",
  "Virgin": "💍",
  "Slayer": "🏹",
  "Soldier": "🛡️",
  "Mayor": "🏛️",
  
  // Outsiders
  "Butler": "🤵",
  "Saint": "👼",
  "Recluse": "🏮",
  "Drunk": "🍺",
  
  // Minions
  "Poisoner": "🧪",
  "Spy": "👓",
  "Baron": "🎩",
  "Scarlet Woman": "💋",
  
  // Demons
  "Imp": "🔱",
  
  // Travellers
  "Scapegoat": "🐐",
  "Gunslinger": "🔫",
  "Beggar": "🥣",
  "Bureaucrat": "📋",
  "Thief": "💎",
  
  // Sects & Violets – Townsfolk
  "Clockmaker": "🕖",
  "Dreamer": "💭",
  "Snake Charmer": "🐍",
  "Mathematician": "🧮",
  "Flowergirl": "🌼",
  "Town Crier": "📯",
  "Oracle": "👁️‍🗨️",
  "Savant": "🦽",
  "Seamstress": "✂️",
  "Philosopher": "🤔",
  "Artist": "🎨",
  "Juggler": "🤹",
  "Sage": "🕯️",
  
  // Sects & Violets – Outsiders
  "Mutant": "🎪",
  "Sweetheart": "🎀",
  "Barber": "💈",
  "Klutz": "🍌",
  
  // Sects & Violets – Minions
  "Evil Twin": "👯",
  "Witch": "🧙‍♀️",
  "Cerenovus": "🧠",
  "Pit‑Hag": "🍲",
  
  // Sects & Violets – Demons
  "Fang Gu": "👐",
  "Vigormortis": "🗝️",
  "No Dashii": "🐙",
  "Vortox": "🌪️",
  
  // Sects & Violets – Travellers
  "Butcher": "🔪",
  "Bone Collector": "🦴",
  "Harlot": "👙",
  "Barista": "☕",
  "Deviant": "🦮",
  
  // Bad Moon Rising – Townsfolk
  "Grandmother": "👵",
  "Sailor": "⚓",
  "Chambermaid": "🧹",
  "Exorcist": "💼",
  "Innkeeper": "🛎️",
  "Gambler": "🎲",
  "Gossip": "🗣️",
  "Courtier": "🍷",
  "Professor": "🎓",
  "Minstrel": "🎶",
  "Tea Lady": "🍵",
  "Pacifist": "🕊️",
  "Fool": "🤡",
  
  // Bad Moon Rising – Outsiders
  "Goon": "👨",
  "Lunatic": "🌀",
  "Tinker": "🔧",
  "Moonchild": "🌙",
  
  // Bad Moon Rising – Minions
  "Godfather": "🌹",
  "Devil's Advocate": "⚖️",
  "Assassin": "🗡️",
  "Mastermind": "🪑",
  
  // Bad Moon Rising – Demons
  "Zombuul": "🧟",
  "Pukka": "😈",
  "Shabaloth": "😃",
  "Po": "💦",

  // Bad Moon Rising – Travellers
  "Apprentice": "🛠️",
  "Matron": "🤶",
  "Voudon": "💀",
  "Judge": "⚖️",
  "Bishop": "♝",
  
  // Experimental - Townsfolk
  "Alchemist": "⚗️",
  "Alsaahir": "🚬",
  "Amnesiac": "❔",
  "Atheist": "🪐",
  "Balloonist": "🎈",
  "Banshee": "👻",
  "Bounty Hunter": "🎯",
  "Cannibal": "🥧",
  "Choirboy": "👦",
  "Cult Leader": "⛛",
  "Engineer": "⚙️",
  "Farmer": "🌾", 
  "Fisherman": "🎣",
  "General": "🎖️",
  "Huntsman": "⛰️",
  "King": "👑",
  "Knight": "♞",
  "Lycanthrope": "🐾",
  "Magician": "🐰",
  "Nightwatchman": "🔦",
  "Noble": "⚜️",
  "Pixie": "🧚",
  "Poppy Grower": "🥀",
  "Preacher": "📓",
  "Shugenja": "⛩️",
  "Steward": "📜",
  "Village Idiot": "🍭",

  // Experimental - Outsiders
  "Acrobat": "🎋",
  "Damsel": "👠",
  "Golem": "🗿",
  "Hatter": "🧢",
  "Heretic": "🙉",
  "Hermit": "⛯",
  "Ogre": "👹",
  "Plague Doctor": "🩺",
  "Politician": "🗳️",
  "Puzzlemaster": "🧩",
  "Snitch": "👄",
  "Zealot": "🍾",

  // Experimental - Minions
  "Boomdandy": "💣",
  "Fearmonger": "😱",
  "Goblin": "😁",
  "Harpy": "🦅",
  "Marionette": "🧵",
  "Mezepheles": "🪶",
  "Organ Grinder": "🐒",
  "Psychopath": "🪓",
  "Summoner": "🙌",
  "Vizier": "🤴",
  "Widow": "🕷️",

  // Exprimental - Demons
  "Al-Hadikhia": "👲",
  "Kazali": "🤖",
  "Legion": "🖐️",
  "Leviathan": "🐋",
  "Lil' Monsta": "👒",
  "Lleech": "🪱",
  "Lord of Typhon": "🐂",
  "Ojo": "👁️",
  "Riot": "🛞",  

  // Expirimental - Travellers
  "Gangster": "🪒",

  // Fabled
  "Doomsayer": "⚡",
  "Angel": "😇",
  "Buddhist": "☸️",
  "Hell's Librarian": "📚",
  "Revolutionary": "✊",
  "Fiddler": "🎻",
  "Toymaker": "🪆",
  "Fibbin": "🩰",
  "Duchess": "👗",
  "Sentinel": "🌉",
  "Spirit of Ivory": "🐘",
  "Djinn": "🪔",
  "Bootlegger": "☠️",
  "Ferryman": "🚣",
  "Gardener": "🏡",
  "Stormcatcher": "🥽",
};

